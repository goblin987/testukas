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
import user # Added import for calling user functions like handle_view_basket
import json # Ensure json is imported


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


        invoice_title_template = lang_data.get("invoice_title_purchase", "*Payment Invoice Created*") if is_purchase else lang_data.get("invoice_title_refill", "*Top\\-Up Invoice Created*")
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
        if is_purchase:
            msg += f"{send_warning_template.format(asset=escaped_currency)}\n"
        else: # It's a refill
            msg += f"{overpayment_note}\n"

        msg += f"\n{confirmation_note}"

        final_msg = msg.strip()
        # Determine correct back button
        back_button_text = back_to_basket_button if is_purchase else back_to_profile_button
        back_callback = "view_basket" if is_purchase else "profile"
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

# --- END OF FILE payment.py ---
