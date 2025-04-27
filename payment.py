# --- START OF FILE payment.py ---

import logging
import sqlite3
import time
import os # Added import
import shutil # Added import
import asyncio
import uuid # For generating unique order IDs
import requests # For making API calls to NOWPayments
from decimal import Decimal, ROUND_UP, ROUND_DOWN # Use Decimal for precision
import json # For parsing potential error messages
from datetime import datetime, timezone # Added import
from collections import Counter, defaultdict # Added import

# --- Telegram Imports ---
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import ContextTypes
from telegram import helpers
import telegram.error as telegram_error
from telegram import InputMediaPhoto, InputMediaVideo, InputMediaAnimation # Import InputMedia types
# -------------------------

# Import necessary items from utils and user
from utils import ( # Ensure utils imports are correct
    send_message_with_retry, format_currency, ADMIN_ID,
    LANGUAGES, load_all_data, BASKET_TIMEOUT, MIN_DEPOSIT_EUR,
    NOWPAYMENTS_API_KEY, NOWPAYMENTS_API_URL, WEBHOOK_URL,
    format_expiration_time, FEE_ADJUSTMENT,
    add_pending_deposit, remove_pending_deposit, # Make sure add_pending_deposit is imported
    get_nowpayments_min_amount,
    get_db_connection, MEDIA_DIR, PRODUCT_TYPES, DEFAULT_PRODUCT_EMOJI, # Added PRODUCT_TYPES/Emoji
    clear_expired_basket # Added import
)
import user # Ensure user module is imported


logger = logging.getLogger(__name__)

# --- NEW: Helper to get NOWPayments Estimate ---
async def _get_nowpayments_estimate(target_eur_amount: Decimal, pay_currency_code: str) -> dict:
    """Gets the estimated crypto amount from NOWPayments API."""
    if not NOWPAYMENTS_API_KEY:
        return {'error': 'payment_api_misconfigured'}

    estimate_url = f"{NOWPAYMENTS_API_URL}/v1/estimate"
    params = {
        'amount': float(target_eur_amount),
        'currency_from': 'eur',
        'currency_to': pay_currency_code.lower()
    }
    headers = {'x-api-key': NOWPAYMENTS_API_KEY}

    try:
        def make_estimate_request():
            try:
                response = requests.get(estimate_url, params=params, headers=headers, timeout=15)
                logger.debug(f"NOWPayments estimate response status: {response.status_code}, content: {response.text[:200]}")
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                logger.error(f"NOWPayments estimate request timed out for {target_eur_amount} EUR to {pay_currency_code}.")
                return {'error': 'estimate_api_timeout'}
            except requests.exceptions.RequestException as e:
                logger.error(f"NOWPayments estimate request error for {target_eur_amount} EUR to {pay_currency_code}: {e}")
                # Try to parse error message if available
                error_detail = str(e)
                if e.response is not None:
                     error_detail = f"Status {e.response.status_code}: {e.response.text[:200]}"
                     if "currencies not found" in e.response.text.lower():
                         return {'error': 'estimate_currency_not_found', 'currency': pay_currency_code.upper()}
                return {'error': 'estimate_api_request_failed', 'details': error_detail}
            except Exception as e:
                 logger.error(f"Unexpected error during NOWPayments estimate call: {e}", exc_info=True)
                 return {'error': 'estimate_api_unexpected_error', 'details': str(e)}

        estimate_data = await asyncio.to_thread(make_estimate_request)

        # Validate response structure
        if 'error' not in estimate_data and 'estimated_amount' not in estimate_data:
             logger.error(f"Invalid estimate response structure: {estimate_data}")
             return {'error': 'invalid_estimate_response'}

        return estimate_data

    except Exception as e:
        logger.error(f"Unexpected error in _get_nowpayments_estimate: {e}", exc_info=True)
        return {'error': 'internal_estimate_error', 'details': str(e)}


# --- Refactored NOWPayments Deposit Creation (Modified) ---
async def create_nowpayments_payment(
    user_id: int,
    target_eur_amount: Decimal,
    pay_currency_code: str,
    is_purchase: bool = False, # <<< ADDED Flag
    basket_snapshot: list | None = None, # <<< ADDED Basket data
    discount_code: str | None = None # <<< ADDED Discount code used
) -> dict:
    """
    Creates a payment invoice using the NOWPayments API.
    Checks minimum amount. Stores extra info if it's a purchase.
    """
    if not NOWPAYMENTS_API_KEY:
        logger.error("NOWPayments API key is not configured.")
        return {'error': 'payment_api_misconfigured'}

    log_type = "direct purchase" if is_purchase else "refill"
    logger.info(f"Attempting to create NOWPayments {log_type} invoice for user {user_id}, {target_eur_amount} EUR via {pay_currency_code}")

    # 1. Get Estimate from NOWPayments
    estimate_result = await _get_nowpayments_estimate(target_eur_amount, pay_currency_code)

    if 'error' in estimate_result:
        logger.error(f"Failed to get estimate for {target_eur_amount} EUR to {pay_currency_code}: {estimate_result}")
        if estimate_result['error'] == 'estimate_currency_not_found':
             return {'error': 'estimate_currency_not_found', 'currency': estimate_result.get('currency', pay_currency_code.upper())}
        return {'error': 'estimate_failed'}

    estimated_crypto_amount = Decimal(str(estimate_result['estimated_amount']))
    logger.info(f"NOWPayments estimated {estimated_crypto_amount} {pay_currency_code} needed for {target_eur_amount} EUR")

    # 2. Check Minimum Payment Amount from NOWPayments
    min_amount_api = get_nowpayments_min_amount(pay_currency_code)
    if min_amount_api is None:
        logger.error(f"Could not fetch minimum payment amount for {pay_currency_code} from NOWPayments API.")
        return {'error': 'min_amount_fetch_error', 'currency': pay_currency_code.upper()}

    invoice_crypto_amount = max(estimated_crypto_amount, min_amount_api)
    if invoice_crypto_amount > estimated_crypto_amount:
        logger.warning(f"Estimated amount {estimated_crypto_amount} was below NOWPayments minimum {min_amount_api}. Using minimum for invoice: {invoice_crypto_amount} {pay_currency_code}")

    # <<< NEW: Check if basket total itself is too low for the *chosen* currency >>>
    # This check happens *before* creating the invoice
    if is_purchase and estimated_crypto_amount < min_amount_api:
         logger.warning(f"Basket purchase for user {user_id} ({target_eur_amount} EUR -> {estimated_crypto_amount} {pay_currency_code}) is below the API minimum {min_amount_api} {pay_currency_code}.")
         # Return a specific error to the user
         return {
             'error': 'basket_pay_too_low',
             'currency': pay_currency_code.upper(),
             'min_amount': f"{min_amount_api:.8f}".rstrip('0').rstrip('.'),
             'basket_total': format_currency(target_eur_amount)
         }
    # <<< END NEW CHECK >>>


    # 3. Prepare API Request Data
    order_id_prefix = "PURCHASE" if is_purchase else "REFILL"
    order_id = f"USER{user_id}_{order_id_prefix}_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    ipn_callback_url = f"{WEBHOOK_URL}/webhook"
    order_desc = f"Basket purchase for user {user_id}" if is_purchase else f"Balance top-up for user {user_id}"

    payload = {
        "price_amount": float(invoice_crypto_amount),
        "price_currency": pay_currency_code.lower(),
        "pay_currency": pay_currency_code.lower(),
        "ipn_callback_url": ipn_callback_url,
        "order_id": order_id,
        "order_description": f"{order_desc} (~{target_eur_amount:.2f} EUR)",
        "is_fixed_rate": False,
    }
    headers = {'x-api-key': NOWPAYMENTS_API_KEY, 'Content-Type': 'application/json'}
    payment_url = f"{NOWPAYMENTS_API_URL}/v1/payment"

    # 4. Make Payment Creation API Call
    try:
        def make_payment_request():
            try:
                response = requests.post(payment_url, headers=headers, json=payload, timeout=20)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                 logger.error(f"NOWPayments payment API request timed out for order {order_id}.")
                 return {'error': 'api_timeout', 'internal': True}
            except requests.exceptions.RequestException as e:
                 logger.error(f"NOWPayments payment API request error for order {order_id}: {e}", exc_info=True)
                 status_code = e.response.status_code if e.response is not None else None
                 error_content = e.response.text if e.response is not None else "No response content"
                 if status_code == 401: return {'error': 'api_key_invalid'}
                 if status_code == 400 and "AMOUNT_MINIMAL_ERROR" in error_content:
                     logger.warning(f"NOWPayments rejected payment for {order_id} due to amount being too low (API check during payment creation).")
                     min_amount_fallback = f"{min_amount_api:.8f}".rstrip('0').rstrip('.')
                     return {'error': 'amount_too_low_api', 'currency': pay_currency_code.upper(), 'min_amount': min_amount_fallback, 'crypto_amount': f"{invoice_crypto_amount:.8f}".rstrip('0').rstrip('.'), 'target_eur_amount': target_eur_amount}
                 return {'error': 'api_request_failed', 'details': str(e), 'status': status_code, 'content': error_content[:200]}
            except Exception as e:
                 logger.error(f"Unexpected error during NOWPayments payment API call for order {order_id}: {e}", exc_info=True)
                 return {'error': 'api_unexpected_error', 'details': str(e)}

        payment_data = await asyncio.to_thread(make_payment_request)
        if 'error' in payment_data:
             if payment_data['error'] == 'api_key_invalid': logger.critical("NOWPayments API Key seems invalid!")
             elif payment_data.get('internal'): logger.error("Internal error during API request (e.g., timeout).")
             elif payment_data['error'] == 'amount_too_low_api': return payment_data
             else: logger.error(f"NOWPayments API returned error during payment creation: {payment_data}")
             return payment_data # Return other errors as well

        # 5. Validate Payment Response
        required_keys = ['payment_id', 'pay_address', 'pay_amount', 'pay_currency', 'expiration_estimate_date']
        if not all(k in payment_data for k in required_keys):
             logger.error(f"Invalid response from NOWPayments payment API for order {order_id}: Missing keys. Response: {payment_data}")
             return {'error': 'invalid_api_response'}

        expected_crypto_amount_from_invoice = Decimal(str(payment_data['pay_amount']))
        payment_data['target_eur_amount_orig'] = float(target_eur_amount)
        payment_data['pay_amount'] = f"{expected_crypto_amount_from_invoice:.8f}".rstrip('0').rstrip('.')
        payment_data['is_purchase'] = is_purchase # <<< ADDED: Pass flag through response for display logic

        # 6. Store Pending Deposit Info (Using modified add_pending_deposit)
        add_success = await asyncio.to_thread(
            add_pending_deposit,
            payment_data['payment_id'], user_id, payment_data['pay_currency'],
            float(target_eur_amount), float(expected_crypto_amount_from_invoice),
            is_purchase=is_purchase,               # <<< Pass flag
            basket_snapshot=basket_snapshot,        # <<< Pass basket
            discount_code=discount_code             # <<< Pass discount code
        )
        if not add_success:
             logger.error(f"Failed to add pending deposit to DB for payment_id {payment_data['payment_id']} (user {user_id}).")
             return {'error': 'pending_db_error'}

        logger.info(f"Successfully created NOWPayments {log_type} invoice {payment_data['payment_id']} for user {user_id}.")
        return payment_data

    except Exception as e:
        logger.error(f"Unexpected error in create_nowpayments_payment for user {user_id}: {e}", exc_info=True)
        return {'error': 'internal_server_error', 'details': str(e)}


# --- Callback Handler for Crypto Selection during Refill ---
async def handle_select_refill_crypto(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the user selecting the crypto asset for refill, creates NOWPayments invoice."""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang = context.user_data.get("lang", "en") # Get language
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    if not params:
        logger.warning(f"handle_select_refill_crypto called without asset parameter for user {user_id}")
        await query.answer("Error: Missing crypto choice.", show_alert=True)
        return

    selected_asset_code = params[0].lower()
    logger.info(f"User {user_id} selected {selected_asset_code} for refill.")

    refill_eur_amount_float = context.user_data.get('refill_eur_amount')
    if not refill_eur_amount_float or refill_eur_amount_float <= 0:
        logger.error(f"Refill amount context lost before asset selection for user {user_id}.")
        await query.edit_message_text("❌ Error: Refill amount context lost. Please start the top up again.", parse_mode=None)
        context.user_data.pop('state', None)
        return

    refill_eur_amount_decimal = Decimal(str(refill_eur_amount_float))

    preparing_invoice_msg = lang_data.get("preparing_invoice", "⏳ Preparing your payment invoice...")
    failed_invoice_creation_msg = lang_data.get("failed_invoice_creation", "❌ Failed to create payment invoice. Please try again later or contact support.")
    error_nowpayments_api_msg = lang_data.get("error_nowpayments_api", "❌ Payment API Error: Could not create payment. Please try again later or contact support.")
    error_invalid_response_msg = lang_data.get("error_invalid_nowpayments_response", "❌ Payment API Error: Invalid response received. Please contact support.")
    error_api_key_msg = lang_data.get("error_nowpayments_api_key", "❌ Payment API Error: Invalid API key. Please contact support.")
    error_pending_db_msg = lang_data.get("payment_pending_db_error", "❌ Database Error: Could not record pending payment. Please contact support.")
    error_amount_too_low_api_msg = lang_data.get("payment_amount_too_low_api", "❌ Payment Amount Too Low: The equivalent of {target_eur_amount} EUR in {currency} ({crypto_amount}) is below the minimum required by the payment provider ({min_amount} {currency}). Please try a higher EUR amount.")
    error_min_amount_fetch_msg = lang_data.get("error_min_amount_fetch", "❌ Error: Could not retrieve minimum payment amount for {currency}. Please try again later or select a different currency.")
    error_estimate_failed_msg = lang_data.get("error_estimate_failed", "❌ Error: Could not estimate crypto amount. Please try again or select a different currency.")
    error_estimate_currency_not_found_msg = lang_data.get("error_estimate_currency_not_found", "❌ Error: Currency {currency} not supported for estimation. Please select a different currency.")
    back_to_profile_button = lang_data.get("back_profile_button", "Back to Profile")
    back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {back_to_profile_button}", callback_data="profile")]])

    try:
        await query.edit_message_text(preparing_invoice_msg, reply_markup=None, parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.warning(f"Couldn't edit message in handle_select_refill_crypto: {e}")
        await query.answer("Preparing...")

    # Call payment creation - specify it's NOT a purchase
    payment_result = await create_nowpayments_payment(
        user_id, refill_eur_amount_decimal, selected_asset_code,
        is_purchase=False # <<< Explicitly False for refill
    )

    if 'error' in payment_result:
        error_code = payment_result['error']
        logger.error(f"Failed to create NOWPayments refill invoice for user {user_id}: {error_code} - Details: {payment_result}")

        error_message_to_user = failed_invoice_creation_msg # Default error
        if error_code == 'estimate_failed': error_message_to_user = error_estimate_failed_msg
        elif error_code == 'estimate_currency_not_found': error_message_to_user = error_estimate_currency_not_found_msg.format(currency=payment_result.get('currency', selected_asset_code.upper()))
        elif error_code == 'min_amount_fetch_error': error_message_to_user = error_min_amount_fetch_msg.format(currency=payment_result.get('currency', selected_asset_code.upper()))
        elif error_code == 'api_key_invalid': error_message_to_user = error_api_key_msg
        elif error_code == 'invalid_api_response': error_message_to_user = error_invalid_response_msg
        elif error_code == 'pending_db_error': error_message_to_user = error_pending_db_msg
        elif error_code == 'amount_too_low_api': # Should ideally not happen for refill unless min deposit is very high
             min_amount_val = payment_result.get('min_amount', 'N/A'); crypto_amount_val = payment_result.get('crypto_amount', 'N/A')
             target_eur_val = payment_result.get('target_eur_amount', refill_eur_amount_decimal)
             error_message_to_user = error_amount_too_low_api_msg.format(target_eur_amount=format_currency(target_eur_val), currency=payment_result.get('currency', selected_asset_code.upper()), crypto_amount=crypto_amount_val, min_amount=min_amount_val)
        elif error_code in ['api_timeout', 'api_request_failed', 'api_unexpected_error', 'internal_server_error', 'internal_estimate_error']:
            error_message_to_user = error_nowpayments_api_msg

        try: await query.edit_message_text(error_message_to_user, reply_markup=back_button_markup, parse_mode=None)
        except Exception as edit_e: logger.error(f"Failed to edit message with invoice creation error: {edit_e}"); await send_message_with_retry(context.bot, chat_id, error_message_to_user, reply_markup=back_button_markup, parse_mode=None)
        context.user_data.pop('refill_eur_amount', None)
        context.user_data.pop('state', None) # Reset state on error
    else:
        logger.info(f"NOWPayments refill invoice created successfully for user {user_id}. Payment ID: {payment_result.get('payment_id')}")
        context.user_data.pop('refill_eur_amount', None)
        context.user_data.pop('state', None)
        await display_nowpayments_invoice(update, context, payment_result)


# --- NEW: Callback Handler for Crypto Selection during Basket Payment ---
async def handle_select_basket_crypto(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the user selecting crypto asset for direct basket payment."""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    if not params:
        logger.warning(f"handle_select_basket_crypto called without asset parameter for user {user_id}")
        await query.answer("Error: Missing crypto choice.", show_alert=True)
        return

    selected_asset_code = params[0].lower()
    logger.info(f"User {user_id} selected {selected_asset_code} for basket payment.")

    # Retrieve stored basket context
    basket_snapshot = context.user_data.get('basket_pay_snapshot')
    total_eur_float = context.user_data.get('basket_pay_total_eur')
    discount_code_used = context.user_data.get('basket_pay_discount_code') # Might be None

    if basket_snapshot is None or total_eur_float is None:
        logger.error(f"Basket payment context lost before crypto selection for user {user_id}.")
        await query.edit_message_text("❌ Error: Payment context lost. Please go back to your basket.",
                                       reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ View Basket", callback_data="view_basket")]]) ,parse_mode=None)
        context.user_data.pop('state', None)
        # Clear potentially stale basket payment context
        context.user_data.pop('basket_pay_snapshot', None)
        context.user_data.pop('basket_pay_total_eur', None)
        context.user_data.pop('basket_pay_discount_code', None)
        return

    basket_total_eur_decimal = Decimal(str(total_eur_float))

    # Get language strings (same as refill for now, potentially customize later)
    preparing_invoice_msg = lang_data.get("preparing_invoice", "⏳ Preparing your payment invoice...")
    failed_invoice_creation_msg = lang_data.get("failed_invoice_creation", "❌ Failed to create payment invoice. Please try again later or contact support.")
    error_nowpayments_api_msg = lang_data.get("error_nowpayments_api", "❌ Payment API Error: Could not create payment. Please try again later or contact support.")
    error_invalid_response_msg = lang_data.get("error_invalid_nowpayments_response", "❌ Payment API Error: Invalid response received. Please contact support.")
    error_api_key_msg = lang_data.get("error_nowpayments_api_key", "❌ Payment API Error: Invalid API key. Please contact support.")
    error_pending_db_msg = lang_data.get("payment_pending_db_error", "❌ Database Error: Could not record pending payment. Please contact support.")
    error_amount_too_low_api_msg = lang_data.get("payment_amount_too_low_api", "❌ Payment Amount Too Low: The equivalent of {target_eur_amount} EUR in {currency} ({crypto_amount}) is below the minimum required by the payment provider ({min_amount} {currency}). Please try a higher EUR amount.")
    error_min_amount_fetch_msg = lang_data.get("error_min_amount_fetch", "❌ Error: Could not retrieve minimum payment amount for {currency}. Please try again later or select a different currency.")
    error_estimate_failed_msg = lang_data.get("error_estimate_failed", "❌ Error: Could not estimate crypto amount. Please try again or select a different currency.")
    error_estimate_currency_not_found_msg = lang_data.get("error_estimate_currency_not_found", "❌ Error: Currency {currency} not supported for estimation. Please select a different currency.")
    error_basket_pay_too_low_msg = lang_data.get("basket_pay_too_low", "❌ Basket total {basket_total} EUR is below the minimum required for {currency}.") # <<< Specific error message
    back_to_basket_button = lang_data.get("back_basket_button", "Back to Basket")
    back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {back_to_basket_button}", callback_data="view_basket")]])

    try:
        await query.edit_message_text(preparing_invoice_msg, reply_markup=None, parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.warning(f"Couldn't edit message in handle_select_basket_crypto: {e}")
        await query.answer("Preparing...")

    # Call payment creation - specify it IS a purchase
    payment_result = await create_nowpayments_payment(
        user_id, basket_total_eur_decimal, selected_asset_code,
        is_purchase=True, # <<< Explicitly TRUE for basket payment
        basket_snapshot=basket_snapshot,
        discount_code=discount_code_used
    )

    # Clear context *after* attempting payment creation
    context.user_data.pop('basket_pay_snapshot', None)
    context.user_data.pop('basket_pay_total_eur', None)
    context.user_data.pop('basket_pay_discount_code', None)
    context.user_data.pop('state', None) # Ensure state is cleared

    if 'error' in payment_result:
        error_code = payment_result['error']
        logger.error(f"Failed to create NOWPayments basket payment invoice for user {user_id}: {error_code} - Details: {payment_result}")

        error_message_to_user = failed_invoice_creation_msg # Default error
        # Handle specific errors
        if error_code == 'basket_pay_too_low': # <<< Handle the new specific error
            error_message_to_user = error_basket_pay_too_low_msg.format(
                basket_total=payment_result.get('basket_total', 'N/A'),
                currency=payment_result.get('currency', selected_asset_code.upper())
            )
        elif error_code == 'estimate_failed': error_message_to_user = error_estimate_failed_msg
        elif error_code == 'estimate_currency_not_found': error_message_to_user = error_estimate_currency_not_found_msg.format(currency=payment_result.get('currency', selected_asset_code.upper()))
        elif error_code == 'min_amount_fetch_error': error_message_to_user = error_min_amount_fetch_msg.format(currency=payment_result.get('currency', selected_asset_code.upper()))
        elif error_code == 'api_key_invalid': error_message_to_user = error_api_key_msg
        elif error_code == 'invalid_api_response': error_message_to_user = error_invalid_response_msg
        elif error_code == 'pending_db_error': error_message_to_user = error_pending_db_msg
        elif error_code == 'amount_too_low_api': # Should ideally not happen due to pre-check, but handle anyway
             min_amount_val = payment_result.get('min_amount', 'N/A'); crypto_amount_val = payment_result.get('crypto_amount', 'N/A')
             target_eur_val = payment_result.get('target_eur_amount', basket_total_eur_decimal)
             error_message_to_user = error_amount_too_low_api_msg.format(target_eur_amount=format_currency(target_eur_val), currency=payment_result.get('currency', selected_asset_code.upper()), crypto_amount=crypto_amount_val, min_amount=min_amount_val)
        elif error_code in ['api_timeout', 'api_request_failed', 'api_unexpected_error', 'internal_server_error', 'internal_estimate_error']:
            error_message_to_user = error_nowpayments_api_msg

        try: await query.edit_message_text(error_message_to_user, reply_markup=back_button_markup, parse_mode=None)
        except Exception as edit_e: logger.error(f"Failed to edit message with basket payment creation error: {edit_e}"); await send_message_with_retry(context.bot, chat_id, error_message_to_user, reply_markup=back_button_markup, parse_mode=None)

        # Since payment failed, the items are still reserved in the user's main basket.
        # We should probably send them back to the basket view.
        await user.handle_view_basket(update, context)

    else:
        logger.info(f"NOWPayments basket payment invoice created successfully for user {user_id}. Payment ID: {payment_result.get('payment_id')}")
        # Display the invoice (same function as refill)
        await display_nowpayments_invoice(update, context, payment_result)
        # Important: DO NOT clear the user's actual basket here.
        # It only gets cleared after the webhook confirms payment.

# --- Display NOWPayments Invoice ---
async def display_nowpayments_invoice(update: Update, context: ContextTypes.DEFAULT_TYPE, payment_data: dict):
    """Displays the NOWPayments invoice details with improved formatting."""
    query = update.callback_query
    chat_id = query.message.chat_id
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    final_msg = "Error displaying invoice."
    is_purchase_invoice = payment_data.get('is_purchase', False) # Check if it's a purchase

    try:
        pay_address = payment_data.get('pay_address')
        pay_amount_str = payment_data.get('pay_amount')
        pay_currency = payment_data.get('pay_currency', 'N/A').upper()
        payment_id = payment_data.get('payment_id', 'N/A')
        target_eur_orig = payment_data.get('target_eur_amount_orig')
        expiration_date_str = payment_data.get('expiration_estimate_date')

        if not pay_address or not pay_amount_str:
            logger.error(f"Missing critical data in NOWPayments response for display: {payment_data}")
            raise ValueError("Missing payment address or amount")

        pay_amount_decimal = Decimal(pay_amount_str)
        pay_amount_display = '{:f}'.format(pay_amount_decimal.normalize())
        target_eur_display = format_currency(Decimal(str(target_eur_orig))) if target_eur_orig else "N/A"
        expiry_time_display = format_expiration_time(expiration_date_str)


        invoice_title_template = lang_data.get("invoice_title_purchase", "*Payment Invoice Created*") if is_purchase_invoice else lang_data.get("invoice_title_refill", "*Top\\-Up Invoice Created*")
        amount_label = lang_data.get("amount_label", "*Amount:*")
        payment_address_label = lang_data.get("payment_address_label", "*Payment Address:*")
        expires_at_label = lang_data.get("expires_at_label", "*Expires At:*")
        send_warning_template = lang_data.get("send_warning_template", "⚠️ *Important:* Send *exactly* this amount of {asset} to this address\\.")
        confirmation_note = lang_data.get("confirmation_note", "✅ Confirmation is automatic via webhook after network confirmation\\.")
        overpayment_note = lang_data.get("overpayment_note", "ℹ️ _Sending more than this amount is okay\\! Your balance will be credited based on the amount received after network confirmation\\._") # Only for refill
        back_to_profile_button = lang_data.get("back_profile_button", "Back to Profile")
        back_to_basket_button = lang_data.get("back_basket_button", "Back to Basket")

        escaped_target_eur = helpers.escape_markdown(target_eur_display, version=2)
        escaped_pay_amount = helpers.escape_markdown(pay_amount_display, version=2)
        escaped_currency = helpers.escape_markdown(pay_currency, version=2)
        escaped_address = helpers.escape_markdown(pay_address, version=2)
        escaped_expiry = helpers.escape_markdown(expiry_time_display, version=2)

        msg = f"""{invoice_title_template}

_{helpers.escape_markdown(f"(Amount: {target_eur_display} EUR)", version=2)}_

Please send the following amount:
{amount_label} `{escaped_pay_amount}` {escaped_currency}

{payment_address_label}
`{escaped_address}`

{expires_at_label} {escaped_expiry}

"""
        # Add relevant notes based on type
        if is_purchase_invoice:
            msg += f"{send_warning_template.format(asset=escaped_currency)}\n"
        else: # It's a refill
            msg += f"{overpayment_note}\n"

        msg += f"\n{confirmation_note}"

        final_msg = msg.strip()
        # Determine correct back button
        back_button_text = back_to_basket_button if is_purchase_invoice else back_to_profile_button
        back_callback = "view_basket" if is_purchase_invoice else "profile"
        keyboard = [[InlineKeyboardButton(f"⬅️ {back_button_text}", callback_data=back_callback)]]

        await query.edit_message_text(
            final_msg, reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode=ParseMode.MARKDOWN_V2, disable_web_page_preview=True
        )
    except (ValueError, KeyError, TypeError) as e:
        logger.error(f"Error formatting or displaying NOWPayments invoice: {e}. Data: {payment_data}", exc_info=True)
        error_display_msg = lang_data.get("error_preparing_payment", "❌ An error occurred while preparing the payment details. Please try again later.")
        # Determine correct back button on error too
        back_button_text = back_to_basket_button if is_purchase_invoice else back_to_profile_button
        back_callback = "view_basket" if is_purchase_invoice else "profile"
        back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {back_button_text}", callback_data=back_callback)]])
        try: await query.edit_message_text(error_display_msg, reply_markup=back_button_markup, parse_mode=None)
        except Exception: pass
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower():
             logger.error(f"Error editing NOWPayments invoice message: {e}. Attempted message (unescaped for logging): {msg.strip()}")
        else: await query.answer()
    except Exception as e:
         logger.error(f"Unexpected error in display_nowpayments_invoice: {e}", exc_info=True)
         error_display_msg = lang_data.get("error_preparing_payment", "❌ An unexpected error occurred while preparing the payment details.")
         back_button_text = back_to_basket_button if is_purchase_invoice else back_to_profile_button
         back_callback = "view_basket" if is_purchase_invoice else "profile"
         back_button_markup = InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ {back_button_text}", callback_data=back_callback)]])
         try: await query.edit_message_text(error_display_msg, reply_markup=back_button_markup, parse_mode=None)
         except Exception: pass


# --- Process Successful Refill (Unchanged) ---
async def process_successful_refill(user_id: int, amount_to_add_eur: Decimal, payment_id: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    bot = context.bot
    user_lang = 'en'
    conn_lang = None
    try:
        conn_lang = get_db_connection()
        c_lang = conn_lang.cursor()
        c_lang.execute("SELECT language FROM users WHERE user_id = ?", (user_id,))
        lang_res = c_lang.fetchone()
        if lang_res and lang_res['language'] in LANGUAGES:
            user_lang = lang_res['language']
    except sqlite3.Error as e:
        logger.error(f"DB error fetching language for user {user_id} during refill confirmation: {e}")
    finally:
        if conn_lang: conn_lang.close()

    lang_data = LANGUAGES.get(user_lang, LANGUAGES['en'])

    if not isinstance(amount_to_add_eur, Decimal) or amount_to_add_eur <= Decimal('0.0'):
        logger.error(f"Invalid amount_to_add_eur in process_successful_refill: {amount_to_add_eur}")
        return False

    conn = None
    db_update_successful = False
    amount_float = float(amount_to_add_eur)
    new_balance = Decimal('0.0')

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN")
        logger.info(f"Attempting balance update for user {user_id} by {amount_float:.2f} EUR (Refill Payment ID: {payment_id})")

        update_result = c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount_float, user_id))
        if update_result.rowcount == 0:
            logger.error(f"User {user_id} not found during refill DB update (Payment ID: {payment_id}). Rowcount: {update_result.rowcount}")
            conn.rollback()
            return False

        c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        new_balance_result = c.fetchone()
        if new_balance_result: new_balance = Decimal(str(new_balance_result['balance']))
        else: logger.error(f"Could not fetch new balance for {user_id} after refill update."); conn.rollback(); return False

        conn.commit()
        db_update_successful = True
        logger.info(f"Successfully processed refill DB update for user {user_id}. Added: {amount_to_add_eur:.2f} EUR. New Balance: {new_balance:.2f} EUR.")

        top_up_success_title = lang_data.get("top_up_success_title", "✅ Top Up Successful!")
        amount_added_label = lang_data.get("amount_added_label", "Amount Added")
        new_balance_label = lang_data.get("new_balance_label", "Your new balance")
        back_to_profile_button = lang_data.get("back_profile_button", "Back to Profile")

        amount_str = format_currency(amount_to_add_eur)
        new_balance_str = format_currency(new_balance)

        success_msg = (f"{top_up_success_title}\n\n{amount_added_label}: {amount_str} EUR\n"
                       f"{new_balance_label}: {new_balance_str} EUR")
        keyboard = [[InlineKeyboardButton(f"👤 {back_to_profile_button}", callback_data="profile")]]

        # Use a dummy context if necessary, or the provided one
        bot_instance = context.bot if hasattr(context, 'bot') else None
        if bot_instance:
            await send_message_with_retry(bot_instance, user_id, success_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        else:
             logger.error(f"Could not get bot instance to notify user {user_id} about refill success.")


        return True

    except sqlite3.Error as e:
        logger.error(f"DB error during process_successful_refill user {user_id} (Payment ID: {payment_id}): {e}", exc_info=True)
        if conn and conn.in_transaction: conn.rollback()
        return False
    except Exception as e:
         logger.error(f"Unexpected error during process_successful_refill user {user_id} (Payment ID: {payment_id}): {e}", exc_info=True)
         if conn and conn.in_transaction: conn.rollback()
         return False
    finally:
        if conn: conn.close()


# --- HELPER: Finalize Purchase (Shared Logic) ---
async def _finalize_purchase(user_id: int, basket_snapshot: list, discount_code_used: str | None, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """
    Shared logic to finalize a purchase after payment confirmation (balance or crypto).
    Decrements stock, adds purchase record, sends details, cleans up product/media.
    """
    chat_id = context._chat_id or context._user_id or user_id # Try to get chat_id
    if not chat_id:
         logger.error(f"Cannot determine chat_id for user {user_id} in _finalize_purchase")
         # This is tricky, we might not be able to notify the user easily here.
         # The purchase might still proceed in DB, but user won't get pickup details via bot.
         # For now, log and proceed with DB operations. A fallback might be needed.

    lang = context.user_data.get("lang", "en") # Get lang from context if available
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    if not basket_snapshot: logger.error(f"Empty basket_snapshot for user {user_id} purchase finalization."); return False

    conn = None
    processed_product_ids = []
    purchases_to_insert = []
    final_pickup_details = defaultdict(list)
    db_update_successful = False
    total_price_paid_decimal = Decimal('0.0')

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN EXCLUSIVE")

        # 1. Process items (Decrement available, check price, prepare purchase record)
        product_ids_in_snapshot = list(set(item['product_id'] for item in basket_snapshot))
        if not product_ids_in_snapshot: logger.warning(f"Empty snapshot IDs user {user_id} finalization."); conn.rollback(); return False

        placeholders = ','.join('?' * len(product_ids_in_snapshot))
        # Fetch details needed for processing and pickup info
        c.execute(f"SELECT id, name, product_type, size, price, city, district, original_text FROM products WHERE id IN ({placeholders})", product_ids_in_snapshot)
        product_db_details = {row['id']: dict(row) for row in c.fetchall()}
        purchase_time_iso = datetime.now(timezone.utc).isoformat()

        for item_snapshot in basket_snapshot:
            product_id = item_snapshot['product_id']
            details = product_db_details.get(product_id)
            # Note: We assume the item *must* exist because it was reserved.
            # If it doesn't, something went wrong previously.
            if not details:
                logger.error(f"CRITICAL: Reserved product {product_id} missing from DB during finalization user {user_id}. Skipping item.")
                continue # Skip this item, but try to process others

            # Decrement available count (already reserved, just need to remove from available)
            # We trust the reservation system and don't check available > reserved here again.
            avail_update = c.execute("UPDATE products SET available = available - 1 WHERE id = ? AND available > 0", (product_id,))
            if avail_update.rowcount == 0:
                # This *shouldn't* happen if reservation worked, but log if it does
                logger.error(f"CRITICAL: Failed available decrement for reserved product P{product_id} user {user_id}. Race condition or logic error?")
                # Don't rollback everything, just skip this item maybe? Or rollback?
                # Let's skip for now, maybe partial purchase is better than none.
                continue

            item_price_decimal = Decimal(str(details['price']))
            total_price_paid_decimal += item_price_decimal # Sum prices for logging/stats
            item_price_float = float(item_price_decimal)

            purchases_to_insert.append((user_id, product_id, details['name'], details['product_type'], details['size'], item_price_float, details['city'], details['district'], purchase_time_iso))
            processed_product_ids.append(product_id)
            final_pickup_details[product_id].append({'name': details['name'], 'size': details['size'], 'text': details.get('original_text')})

        if not purchases_to_insert:
            logger.warning(f"No items processed during finalization for user {user_id}. Rolling back.")
            conn.rollback()
            if chat_id: await send_message_with_retry(context.bot, chat_id, lang_data.get("error_processing_purchase_contact_support", "❌ Error processing purchase."), parse_mode=None)
            return False

        # 2. Record Purchases & Update User Stats
        c.executemany("INSERT INTO purchases (user_id, product_id, product_name, product_type, product_size, price_paid, city, district, purchase_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", purchases_to_insert)
        c.execute("UPDATE users SET total_purchases = total_purchases + ? WHERE user_id = ?", (len(purchases_to_insert), user_id))

        # Increment discount code usage if applicable
        if discount_code_used:
            logger.info(f"Incrementing usage count for discount code '{discount_code_used}' used by {user_id}.")
            c.execute("UPDATE discount_codes SET uses_count = uses_count + 1 WHERE code = ?", (discount_code_used,))

        # Clear user's basket in DB
        c.execute("UPDATE users SET basket = '' WHERE user_id = ?", (user_id,))
        conn.commit()
        db_update_successful = True
        logger.info(f"Finalized purchase DB update user {user_id}. Processed {len(purchases_to_insert)} items. Discount: {discount_code_used or 'None'}")

    except sqlite3.Error as e:
        logger.error(f"DB error during purchase finalization user {user_id}: {e}", exc_info=True); db_update_successful = False
        if conn and conn.in_transaction: conn.rollback()
    except Exception as e:
        logger.error(f"Unexpected error during purchase finalization user {user_id}: {e}", exc_info=True); db_update_successful = False
        if conn and conn.in_transaction: conn.rollback()
    finally:
        if conn: conn.close()

    # --- Post-Transaction Cleanup & Message Sending (If DB success) ---
    if db_update_successful:
        # Clear context basket and discount
        context.user_data['basket'] = []
        context.user_data.pop('applied_discount', None)

        # Fetch Media (same as before)
        media_details = defaultdict(list)
        if processed_product_ids:
            conn_media = None
            try:
                conn_media = get_db_connection()
                c_media = conn_media.cursor()
                media_placeholders = ','.join('?' * len(processed_product_ids))
                c_media.execute(f"SELECT product_id, media_type, telegram_file_id, file_path FROM product_media WHERE product_id IN ({media_placeholders})", processed_product_ids)
                for row in c_media.fetchall(): media_details[row['product_id']].append(dict(row))
            except sqlite3.Error as e: logger.error(f"DB error fetching media post-purchase: {e}")
            finally:
                if conn_media: conn_media.close()

        # Send Pickup Details (same as before)
        if chat_id: # Only attempt if we have a chat_id
            success_title = lang_data.get("purchase_success", "🎉 Purchase Complete! Pickup details below:")
            await send_message_with_retry(context.bot, chat_id, success_title, parse_mode=None)

            for prod_id in processed_product_ids:
                item_details_list = final_pickup_details.get(prod_id)
                if not item_details_list: continue
                item_details = item_details_list[0] # Assuming one entry per prod_id here
                item_name, item_size = item_details['name'], item_details['size']
                item_text = item_details['text'] or "(No specific pickup details provided)"
                product_type = product_db_details.get(prod_id, {}).get('product_type', 'Product') # Get type for emoji
                product_emoji = PRODUCT_TYPES.get(product_type, DEFAULT_PRODUCT_EMOJI)
                item_header = f"--- Item: {product_emoji} {item_name} {item_size} ---"

                media_sent = False; caption_sent_with_media = False; opened_files = []
                if prod_id in media_details:
                    # ... (Keep the exact media sending logic from process_purchase_with_balance) ...
                     media_list = media_details[prod_id]
                     if media_list:
                        media_group_to_send = []
                        combined_caption = f"{item_header}\n\n{item_text}"
                        if len(combined_caption) > 1024: combined_caption = combined_caption[:1021] + "..."
                        try:
                            for i, media_item in enumerate(media_list):
                                file_id = media_item.get('telegram_file_id')
                                media_type = media_item.get('media_type')
                                file_path = media_item.get('file_path')
                                caption_to_use = combined_caption if i == 0 else None
                                input_media = None; file_handle = None
                                try:
                                    if file_id:
                                        if media_type == 'photo': input_media = InputMediaPhoto(media=file_id, caption=caption_to_use, parse_mode=None)
                                        elif media_type == 'video': input_media = InputMediaVideo(media=file_id, caption=caption_to_use, parse_mode=None)
                                        elif media_type == 'gif': input_media = InputMediaAnimation(media=file_id, caption=caption_to_use, parse_mode=None)
                                        else: logger.warning(f"Unsupported media type '{media_type}' with file_id P{prod_id}"); continue
                                    elif file_path and await asyncio.to_thread(os.path.exists, file_path):
                                        logger.info(f"Opening media file {file_path} P{prod_id} for sending")
                                        file_handle = await asyncio.to_thread(open, file_path, 'rb')
                                        opened_files.append(file_handle)
                                        if media_type == 'photo': input_media = InputMediaPhoto(media=file_handle, caption=caption_to_use, parse_mode=None)
                                        elif media_type == 'video': input_media = InputMediaVideo(media=file_handle, caption=caption_to_use, parse_mode=None)
                                        elif media_type == 'gif': input_media = InputMediaAnimation(media=file_handle, caption=caption_to_use, parse_mode=None)
                                        else: logger.warning(f"Unsupported media type '{media_type}' from path {file_path}"); await asyncio.to_thread(file_handle.close); opened_files.remove(file_handle); continue
                                    else: logger.warning(f"Media item invalid P{prod_id}: No file_id and path '{file_path}' missing."); continue
                                    if input_media: media_group_to_send.append(input_media)
                                except Exception as prep_e:
                                    logger.error(f"Error preparing media item {i+1} P{prod_id}: {prep_e}", exc_info=True)
                                    if file_handle and file_handle in opened_files: await asyncio.to_thread(file_handle.close); opened_files.remove(file_handle)
                            if media_group_to_send:
                                await context.bot.send_media_group(chat_id, media=media_group_to_send, connect_timeout=20, read_timeout=20)
                                logger.info(f"Sent media group with {len(media_group_to_send)} items for P{prod_id} to user {user_id}.")
                                media_sent = True
                                if media_group_to_send[0].caption: caption_sent_with_media = True
                        except telegram_error.TelegramError as tg_err: logger.error(f"TelegramError sending media group for P{prod_id} to user {user_id}: {tg_err}"); caption_sent_with_media = False
                        except Exception as e: logger.error(f"Unexpected error sending media group for P{prod_id} user {user_id}: {e}", exc_info=True); caption_sent_with_media = False
                        finally:
                            for f in opened_files:
                                try:
                                    if not f.closed: await asyncio.to_thread(f.close); logger.debug(f"Closed file handle during cleanup: {getattr(f, 'name', 'unknown')}")
                                except Exception as close_e: logger.warning(f"Error closing file handle '{getattr(f, 'name', 'unknown')}' during cleanup: {close_e}")

                # Send Text Details ONLY if no media or caption failed
                if not media_sent or not caption_sent_with_media:
                    text_to_send = item_text if media_sent else f"{item_header}\n\n{item_text}"
                    if not text_to_send: text_to_send = f"(No details for {item_name} {item_size})"
                    await send_message_with_retry(context.bot, chat_id, text_to_send, parse_mode=None)
        # Delete Product Records and Media Directories Async (same as before)
        conn_del = None
        try:
            conn_del = get_db_connection()
            c_del = conn_del.cursor()
            # Use executemany for potentially better performance if many items
            ids_tuple_list = [(pid,) for pid in processed_product_ids]
            c_del.executemany("DELETE FROM product_media WHERE product_id = ?", ids_tuple_list)
            delete_result = c_del.executemany("DELETE FROM products WHERE id = ?", ids_tuple_list)
            conn_del.commit()
            deleted_count = delete_result.rowcount # Note: executemany might return -1 or None depending on driver
            logger.info(f"Attempted deletion of {len(processed_product_ids)} purchased product records (Result: {deleted_count}).")
            for prod_id in processed_product_ids:
                 media_dir_to_delete = os.path.join(MEDIA_DIR, str(prod_id))
                 if await asyncio.to_thread(os.path.exists, media_dir_to_delete):
                     asyncio.create_task(asyncio.to_thread(shutil.rmtree, media_dir_to_delete, ignore_errors=True))
                     logger.info(f"Scheduled deletion of media dir: {media_dir_to_delete}")
        except sqlite3.Error as e: logger.error(f"DB error deleting purchased products: {e}", exc_info=True); conn_del.rollback() if conn_del and conn_del.in_transaction else None
        except Exception as e: logger.error(f"Unexpected error deleting purchased products: {e}", exc_info=True)
        finally:
            if conn_del: conn_del.close()

        # Final Message (same as before)
        if chat_id:
             final_message_parts = ["Purchase details sent above."]
             leave_review_button = lang_data.get("leave_review_button", "Leave a Review")
             keyboard = [[InlineKeyboardButton(f"✍️ {leave_review_button}", callback_data="leave_review_now")]]
             await send_message_with_retry(context.bot, chat_id, "\n\n".join(final_message_parts), reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

        return True # Indicate success
    else: # Purchase failed at DB level
        # Basket is likely inconsistent now, maybe clear it? Or try to revert?
        # Safest is often to clear and inform user.
        context.user_data['basket'] = []
        context.user_data.pop('applied_discount', None)
        if chat_id: await send_message_with_retry(context.bot, chat_id, lang_data.get("error_processing_purchase_contact_support", "❌ Error processing purchase."), parse_mode=None)
        return False


# --- Process Purchase with Balance (Uses Helper) ---
async def process_purchase_with_balance(user_id: int, amount_to_deduct: Decimal, basket_snapshot: list, discount_code_used: str | None, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handles DB updates when paying with internal balance."""
    chat_id = context._chat_id or context._user_id or user_id
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    if not basket_snapshot: logger.error(f"Empty basket_snapshot for user {user_id} balance purchase."); return False
    if not isinstance(amount_to_deduct, Decimal) or amount_to_deduct < Decimal('0.0'): logger.error(f"Invalid amount_to_deduct {amount_to_deduct}."); return False

    conn = None
    db_balance_deducted = False
    balance_changed_error = lang_data.get("balance_changed_error", "❌ Transaction failed: Balance changed.")
    error_processing_purchase_contact_support = lang_data.get("error_processing_purchase_contact_support", "❌ Error processing purchase. Contact support.")

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN EXCLUSIVE")
        # 1. Verify balance
        c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        current_balance_result = c.fetchone()
        if not current_balance_result or Decimal(str(current_balance_result['balance'])) < amount_to_deduct:
             logger.warning(f"Insufficient balance user {user_id}. Needed: {amount_to_deduct:.2f}")
             conn.rollback()
             if chat_id: await send_message_with_retry(context.bot, chat_id, balance_changed_error, parse_mode=None)
             return False
        # 2. Deduct balance
        amount_float_to_deduct = float(amount_to_deduct)
        update_res = c.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (amount_float_to_deduct, user_id))
        if update_res.rowcount == 0: logger.error(f"Failed to deduct balance user {user_id}."); conn.rollback(); return False

        conn.commit() # Commit balance deduction *before* finalizing items
        db_balance_deducted = True
        logger.info(f"Deducted {amount_to_deduct:.2f} EUR from balance for user {user_id}.")

    except sqlite3.Error as e:
        logger.error(f"DB error deducting balance user {user_id}: {e}", exc_info=True); db_balance_deducted = False
        if conn and conn.in_transaction: conn.rollback()
    finally:
        if conn: conn.close()

    # 3. Finalize purchase ONLY if balance was successfully deducted
    if db_balance_deducted:
        logger.info(f"Calling _finalize_purchase for user {user_id} after balance deduction.")
        # Now call the shared finalization logic
        finalize_success = await _finalize_purchase(user_id, basket_snapshot, discount_code_used, context)
        return finalize_success
    else:
        logger.error(f"Skipping purchase finalization for user {user_id} due to balance deduction failure.")
        if chat_id: await send_message_with_retry(context.bot, chat_id, error_processing_purchase_contact_support, parse_mode=None)
        return False

# --- NEW: Process Successful Crypto Purchase (Uses Helper) ---
async def process_successful_crypto_purchase(user_id: int, basket_snapshot: list, discount_code_used: str | None, payment_id: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Handles finalizing a purchase paid via crypto webhook."""
    chat_id = context._chat_id or context._user_id or user_id # Try to get chat_id
    lang = context.user_data.get("lang", "en")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])

    logger.info(f"Processing successful crypto purchase for user {user_id}, payment {payment_id}. Basket items: {len(basket_snapshot) if basket_snapshot else 0}")

    if not basket_snapshot:
        logger.error(f"CRITICAL: Successful crypto payment {payment_id} for user {user_id} received, but basket snapshot was empty/missing in pending record.")
        # Cannot finalize purchase without knowing what was bought. Manual intervention likely needed.
        if ADMIN_ID and chat_id:
            try:
                await send_message_with_retry(context.bot, ADMIN_ID, f"⚠️ Critical Issue: Crypto payment {payment_id} success for user {user_id}, but basket data missing! Manual check needed.", parse_mode=None)
            except Exception as admin_notify_e:
                logger.error(f"Failed to notify admin about critical missing basket data: {admin_notify_e}")
        return False # Cannot proceed

    # Call the shared finalization logic
    finalize_success = await _finalize_purchase(user_id, basket_snapshot, discount_code_used, context)

    if finalize_success:
        if chat_id: # Notify user if possible
             success_msg = lang_data.get("crypto_purchase_success", "Payment Confirmed! Your purchase details are being sent.")
             await send_message_with_retry(context.bot, chat_id, success_msg, parse_mode=None)
    else:
        # Finalization failed even after payment confirmed. This is bad.
        logger.error(f"CRITICAL: Crypto payment {payment_id} success for user {user_id}, but _finalize_purchase failed! Items paid for but not processed in DB correctly.")
        if ADMIN_ID and chat_id:
            try:
                await send_message_with_retry(context.bot, ADMIN_ID, f"⚠️ Critical Issue: Crypto payment {payment_id} success for user {user_id}, but finalization FAILED! Manual check/correction needed.", parse_mode=None)
            except Exception as admin_notify_e:
                 logger.error(f"Failed to notify admin about critical finalization failure: {admin_notify_e}")
        if chat_id:
            await send_message_with_retry(context.bot, chat_id, lang_data.get("error_processing_purchase_contact_support", "❌ Error processing purchase. Contact support."), parse_mode=None)


    return finalize_success

# --- Callback Handler Wrapper (to keep main.py structure) ---
async def handle_confirm_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """
    This is a wrapper function.
    The main logic for confirm_pay is now in user.py.
    This function ensures the callback router in main.py finds a handler here.
    """
    logger.debug("Payment.handle_confirm_pay called, forwarding to user.handle_confirm_pay")
    # Call the actual handler which is now located in user.py
    await user.handle_confirm_pay(update, context, params)

# --- END OF FILE payment.py ---
