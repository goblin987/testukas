# --- START OF FILE main.py ---

import logging
import asyncio
import os
import signal
import sqlite3 # Keep for error handling if needed directly
from functools import wraps
from datetime import timedelta
import threading # Added for Flask thread
import json # Added for webhook processing
from decimal import Decimal, ROUND_DOWN, ROUND_UP # <-- MODIFIED: Import ROUND_DOWN and ROUND_UP
# *** ADD THESE IMPORTS for webhook verification ***
import hmac
import hashlib
# ***********************************************


# --- Telegram Imports ---
from telegram import Update, BotCommand, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardMarkup
from telegram.ext import (
    Application, ApplicationBuilder, Defaults, ContextTypes,
    CommandHandler, CallbackQueryHandler, MessageHandler, filters,
    PicklePersistence, JobQueue
)
from telegram.constants import ParseMode
# *** FIXED: Import specific error classes ***
from telegram.error import Forbidden, BadRequest, NetworkError, RetryAfter, TelegramError

# --- Flask Imports ---
from flask import Flask, request, Response # Added for webhook server
import nest_asyncio # Added to allow nested asyncio loops

# --- Local Imports ---
# Import variables/functions that were modified or needed
from utils import (
    TOKEN, ADMIN_ID, init_db, load_all_data, LANGUAGES, THEMES,
    SUPPORT_USERNAME, BASKET_TIMEOUT, clear_all_expired_baskets,
    SECONDARY_ADMIN_IDS, WEBHOOK_URL, # Added WEBHOOK_URL
    # *** ADD NOWPAYMENTS_IPN_SECRET import ***
    NOWPAYMENTS_IPN_SECRET,
    # *************************************
    get_db_connection, # Import the DB connection helper
    DATABASE_PATH, # Import DB path if needed for direct error checks (optional)
    get_pending_deposit, remove_pending_deposit, FEE_ADJUSTMENT, # Import deposit/price utils
    send_message_with_retry, # Import send_message_with_retry
    log_admin_action # Import admin logging
)
from user import (
    start, handle_shop, handle_city_selection, handle_district_selection,
    handle_type_selection, handle_product_selection, handle_add_to_basket,
    handle_view_basket, handle_clear_basket, handle_remove_from_basket,
    handle_profile, handle_language_selection, handle_price_list,
    handle_price_list_city, handle_reviews_menu, handle_leave_review,
    handle_view_reviews, handle_leave_review_message, handle_back_start,
    handle_user_discount_code_message, apply_discount_start, remove_discount,
    handle_leave_review_now, handle_refill, handle_view_history,
    handle_refill_amount_message, validate_discount_code,
    # <<< NEW Basket Payment Handlers >>>
    handle_apply_discount_basket_pay,
    handle_skip_discount_basket_pay,
    handle_basket_discount_code_message,
    _show_crypto_choices_for_basket # Import the helper if needed directly (though unlikely)
    # <<< NOTE: user.handle_confirm_pay is NOT imported here, it's called via payment.handle_confirm_pay >>>
)
from admin import (
    handle_admin_menu, handle_sales_analytics_menu, handle_sales_dashboard,
    handle_sales_select_period, handle_sales_run, handle_adm_city, handle_adm_dist,
    handle_adm_type, handle_adm_add, handle_adm_size, handle_adm_custom_size,
    handle_confirm_add_drop, cancel_add, handle_adm_manage_cities, handle_adm_add_city,
    handle_adm_edit_city, handle_adm_delete_city, handle_adm_manage_districts,
    handle_adm_manage_districts_city, handle_adm_add_district, handle_adm_edit_district,
    handle_adm_remove_district, handle_adm_manage_products, handle_adm_manage_products_city,
    handle_adm_manage_products_dist, handle_adm_manage_products_type, handle_adm_delete_prod,
    handle_adm_manage_types, handle_adm_add_type, handle_adm_delete_type,
    handle_adm_edit_type_menu, handle_adm_change_type_emoji, # <-- Import new type edit handlers
    handle_adm_manage_discounts, handle_adm_toggle_discount, handle_adm_delete_discount,
    handle_adm_add_discount_start, handle_adm_use_generated_code, handle_adm_set_discount_type,
    handle_adm_set_media,
    handle_adm_broadcast_start, handle_cancel_broadcast,
    handle_confirm_broadcast, handle_adm_broadcast_message,
    # --- Broadcast Handlers ---
    handle_adm_broadcast_target_type, handle_adm_broadcast_target_city, handle_adm_broadcast_target_status,
    handle_adm_broadcast_inactive_days_message, # Message handler
    # ----------------------------
    handle_confirm_yes,
    handle_adm_add_city_message,
    handle_adm_add_district_message, handle_adm_edit_district_message,
    handle_adm_edit_city_message, handle_adm_custom_size_message, handle_adm_price_message,
    handle_adm_drop_details_message, handle_adm_bot_media_message, handle_adm_add_type_message,
    handle_adm_add_type_emoji_message, # <-- Import new type emoji handler
    handle_adm_edit_type_emoji_message, # <-- Import new type emoji edit handler
    process_discount_code_input, handle_adm_discount_code_message, handle_adm_discount_value_message,
    handle_adm_manage_reviews, handle_adm_delete_review_confirm,
    # <<< Welcome Message Handlers >>>
    handle_adm_manage_welcome,
    handle_adm_activate_welcome,
    handle_adm_add_welcome_start,
    handle_adm_edit_welcome,
    handle_adm_delete_welcome_confirm,
    handle_adm_welcome_template_name_message, # Message handler
    handle_adm_welcome_template_text_message,   # Message handler
    handle_adm_edit_welcome_text,           # <<< Add this import
    handle_reset_default_welcome,         # <<< Add this import
    # <<< NEW Welcome Save/Preview Handlers (if needed directly, usually not) >>>
    # _show_welcome_preview, # Usually internal to admin.py
    handle_confirm_save_welcome,          # <<< Add this import (for save button)
    # <<< NEW Description Edit Handlers (if needed directly, usually not) >>>
    handle_adm_edit_welcome_desc,           # <<< Add this import
    handle_adm_welcome_description_message, # Message Handler
    handle_adm_welcome_description_edit_message # Message Handler
)
from viewer_admin import (
    handle_viewer_admin_menu,
    handle_viewer_added_products,
    handle_viewer_view_product_media,
    # --- User Management Handlers (Imported from viewer_admin.py) ---
    handle_manage_users_start,
    handle_view_user_profile,
    handle_adjust_balance_start,
    handle_toggle_ban_user,
    handle_adjust_balance_amount_message, # Message handler
    handle_adjust_balance_reason_message # Message handler
    # ---------------------------------------------------------------
)
# --- Import Reseller Management Handlers --- # <<< ADDED
try:
    from reseller_management import (
        handle_manage_resellers_menu,
        handle_reseller_manage_id_message,
        handle_reseller_toggle_status,
        handle_manage_reseller_discounts_select_reseller,
        handle_manage_specific_reseller_discounts,
        handle_reseller_add_discount_select_type,
        handle_reseller_add_discount_enter_percent,
        handle_reseller_edit_discount,
        handle_reseller_percent_message,
        handle_reseller_delete_discount_confirm,
    )
except ImportError:
    logger_dummy_reseller = logging.getLogger(__name__ + "_dummy_reseller")
    logger_dummy_reseller.error("Could not import handlers from reseller_management.py.")
    async def handle_manage_resellers_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
        query = update.callback_query; msg = "Reseller Status Mgmt handler not found."
        if query: await query.edit_message_text(msg)
        else: await send_message_with_retry(context.bot, update.effective_chat.id, msg)
    async def handle_manage_reseller_discounts_select_reseller(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
        query = update.callback_query; msg = "Reseller Discount Mgmt handler not found."
        if query: await query.edit_message_text(msg)
        else: await send_message_with_retry(context.bot, update.effective_chat.id, msg)
    async def handle_reseller_manage_id_message(update: Update, context: ContextTypes.DEFAULT_TYPE): pass
    async def handle_reseller_toggle_status(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_manage_specific_reseller_discounts(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_reseller_add_discount_select_type(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_reseller_add_discount_enter_percent(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_reseller_edit_discount(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
    async def handle_reseller_percent_message(update: Update, context: ContextTypes.DEFAULT_TYPE): pass
    async def handle_reseller_delete_discount_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None): pass
# ------------------------------------------ # <<< END ADDED


# Import payment module for processing refill AND the wrapper
import payment # <<< Imports payment module
from stock import handle_view_stock

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger('apscheduler.scheduler').setLevel(logging.WARNING)
logging.getLogger('apscheduler.executors.default').setLevel(logging.WARNING)
logging.getLogger('werkzeug').setLevel(logging.WARNING) # Silence Flask's default logger
logger = logging.getLogger(__name__)

# Apply nest_asyncio to allow running Flask within the bot's async loop
nest_asyncio.apply()

# --- Globals for Flask & Telegram App ---
flask_app = Flask(__name__)
telegram_app: Application | None = None # Initialize as None
main_loop = None # Store the main event loop

# --- Callback Data Parsing Decorator ---
def callback_query_router(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        if query and query.data:
            parts = query.data.split('|')
            command = parts[0]
            params = parts[1:]
            target_func_name = f"handle_{command}"

            # Map command strings to the actual function objects
            KNOWN_HANDLERS = {
                # User Handlers
                "start": start, "back_start": handle_back_start, "shop": handle_shop,
                "city": handle_city_selection, "dist": handle_district_selection,
                "type": handle_type_selection, "product": handle_product_selection,
                "add": handle_add_to_basket, "view_basket": handle_view_basket,
                "clear_basket": handle_clear_basket, "remove": handle_remove_from_basket,
                "profile": handle_profile, "language": handle_language_selection,
                "price_list": handle_price_list, "price_list_city": handle_price_list_city,
                "reviews": handle_reviews_menu, "leave_review": handle_leave_review,
                "view_reviews": handle_view_reviews, "leave_review_now": handle_leave_review_now,
                "refill": handle_refill,
                "view_history": handle_view_history,
                "apply_discount_start": apply_discount_start, "remove_discount": remove_discount,
                # Basket Payment Flow Handlers
                "confirm_pay": payment.handle_confirm_pay, # <<< POINTS TO PAYMENT WRAPPER
                "apply_discount_basket_pay": handle_apply_discount_basket_pay,
                "skip_discount_basket_pay": handle_skip_discount_basket_pay,
                "select_basket_crypto": payment.handle_select_basket_crypto,
                # Refill Flow Handlers
                "select_refill_crypto": payment.handle_select_refill_crypto,
                # Primary Admin Handlers
                "admin_menu": handle_admin_menu,
                "sales_analytics_menu": handle_sales_analytics_menu, "sales_dashboard": handle_sales_dashboard,
                "sales_select_period": handle_sales_select_period, "sales_run": handle_sales_run,
                "adm_city": handle_adm_city, "adm_dist": handle_adm_dist, "adm_type": handle_adm_type,
                "adm_add": handle_adm_add, "adm_size": handle_adm_size, "adm_custom_size": handle_adm_custom_size,
                "confirm_add_drop": handle_confirm_add_drop, "cancel_add": cancel_add,
                "adm_manage_cities": handle_adm_manage_cities, "adm_add_city": handle_adm_add_city,
                "adm_edit_city": handle_adm_edit_city, "adm_delete_city": handle_adm_delete_city,
                "adm_manage_districts": handle_adm_manage_districts, "adm_manage_districts_city": handle_adm_manage_districts_city,
                "adm_add_district": handle_adm_add_district, "adm_edit_district": handle_adm_edit_district,
                "adm_remove_district": handle_adm_remove_district,
                "adm_manage_products": handle_adm_manage_products, "adm_manage_products_city": handle_adm_manage_products_city,
                "adm_manage_products_dist": handle_adm_manage_products_dist, "adm_manage_products_type": handle_adm_manage_products_type,
                "adm_delete_prod": handle_adm_delete_prod,
                "adm_manage_types": handle_adm_manage_types,
                "adm_edit_type_menu": handle_adm_edit_type_menu,
                "adm_change_type_emoji": handle_adm_change_type_emoji,
                "adm_add_type": handle_adm_add_type,
                "adm_delete_type": handle_adm_delete_type,
                "adm_manage_discounts": handle_adm_manage_discounts, "adm_toggle_discount": handle_adm_toggle_discount,
                "adm_delete_discount": handle_adm_delete_discount, "adm_add_discount_start": handle_adm_add_discount_start,
                "adm_use_generated_code": handle_adm_use_generated_code, "adm_set_discount_type": handle_adm_set_discount_type,
                "adm_set_media": handle_adm_set_media,
                "confirm_yes": handle_confirm_yes,
                # --- Broadcast Handlers ---
                "adm_broadcast_start": handle_adm_broadcast_start,
                "adm_broadcast_target_type": handle_adm_broadcast_target_type,
                "adm_broadcast_target_city": handle_adm_broadcast_target_city,
                "adm_broadcast_target_status": handle_adm_broadcast_target_status,
                "cancel_broadcast": handle_cancel_broadcast,
                "confirm_broadcast": handle_confirm_broadcast,
                # --------------------------
                "adm_manage_reviews": handle_adm_manage_reviews,
                "adm_delete_review_confirm": handle_adm_delete_review_confirm,
                # <<< Welcome Message Callbacks >>>
                "adm_manage_welcome": handle_adm_manage_welcome,
                "adm_activate_welcome": handle_adm_activate_welcome,
                "adm_add_welcome_start": handle_adm_add_welcome_start,
                "adm_edit_welcome": handle_adm_edit_welcome,
                "adm_delete_welcome_confirm": handle_adm_delete_welcome_confirm,
                "adm_edit_welcome_text": handle_adm_edit_welcome_text, # <<< ADDED
                "adm_edit_welcome_desc": handle_adm_edit_welcome_desc, # <<< ADDED
                "adm_reset_default_confirm": handle_reset_default_welcome, # <<< ADDED
                "confirm_save_welcome": handle_confirm_save_welcome, # <<< ADDED
                # -------------------------------
                # --- User Management Callbacks ---
                "adm_manage_users": handle_manage_users_start,
                "adm_view_user": handle_view_user_profile,
                "adm_adjust_balance_start": handle_adjust_balance_start,
                "adm_toggle_ban": handle_toggle_ban_user,
                # -----------------------------------
                # <<< Reseller Management Callbacks >>> # <<< ADDED
                "manage_resellers_menu": handle_manage_resellers_menu,
                "reseller_toggle_status": handle_reseller_toggle_status,
                "manage_reseller_discounts_select_reseller": handle_manage_reseller_discounts_select_reseller,
                "reseller_manage_specific": handle_manage_specific_reseller_discounts,
                "reseller_add_discount_select_type": handle_reseller_add_discount_select_type,
                "reseller_add_discount_enter_percent": handle_reseller_add_discount_enter_percent,
                "reseller_edit_discount": handle_reseller_edit_discount,
                "reseller_delete_discount_confirm": handle_reseller_delete_discount_confirm,
                # ----------------------------------- # <<< END ADDED
                # Stock Handler
                "view_stock": handle_view_stock,
                # Viewer Admin Handlers
                "viewer_admin_menu": handle_viewer_admin_menu,
                "viewer_added_products": handle_viewer_added_products,
                "viewer_view_product_media": handle_viewer_view_product_media
            }

            target_func = KNOWN_HANDLERS.get(command)

            if target_func and asyncio.iscoroutinefunction(target_func):
                await target_func(update, context, params)
            else:
                logger.warning(f"No async handler function found or mapped for callback command: {command}")
                try: await query.answer("Unknown action.", show_alert=True)
                except Exception as e: logger.error(f"Error answering unknown callback query {command}: {e}")
        elif query:
            logger.warning("Callback query handler received update without data.")
            try: await query.answer()
            except Exception as e: logger.error(f"Error answering callback query without data: {e}")
        else:
            logger.warning("Callback query handler received update without query object.")
    return wrapper

@callback_query_router
async def handle_callback_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # This function is now primarily a dispatcher via the decorator.
    pass # Decorator handles everything

# --- Central Message Handler (for states) ---
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles regular messages based on user state."""
    if not update.message or not update.effective_user: return

    user_id = update.effective_user.id
    state = context.user_data.get('state')
    logger.debug(f"Message received from user {user_id}, state: {state}")

    STATE_HANDLERS = {
        'awaiting_review': handle_leave_review_message,
        'awaiting_user_discount_code': handle_user_discount_code_message,
        'awaiting_basket_discount_code': handle_basket_discount_code_message,
        # Admin Message Handlers
        'awaiting_new_city_name': handle_adm_add_city_message,
        'awaiting_edit_city_name': handle_adm_edit_city_message,
        'awaiting_new_district_name': handle_adm_add_district_message,
        'awaiting_edit_district_name': handle_adm_edit_district_message,
        'awaiting_new_type_name': handle_adm_add_type_message,
        'awaiting_new_type_emoji': handle_adm_add_type_emoji_message,
        'awaiting_edit_type_emoji': handle_adm_edit_type_emoji_message,
        'awaiting_custom_size': handle_adm_custom_size_message,
        'awaiting_price': handle_adm_price_message,
        'awaiting_drop_details': handle_adm_drop_details_message,
        'awaiting_bot_media': handle_adm_bot_media_message,
        # --- Broadcast Handlers ---
        'awaiting_broadcast_inactive_days': handle_adm_broadcast_inactive_days_message,
        'awaiting_broadcast_message': handle_adm_broadcast_message,
        # --------------------------
        'awaiting_discount_code': handle_adm_discount_code_message,
        'awaiting_discount_value': handle_adm_discount_value_message,
        # --- Welcome Message States ---
        'awaiting_welcome_template_name': handle_adm_welcome_template_name_message,
        'awaiting_welcome_template_text': handle_adm_welcome_template_text_message,
        'awaiting_welcome_template_edit': handle_adm_welcome_template_text_message,
        'awaiting_welcome_description': handle_adm_welcome_description_message, # <<< ADDED
        'awaiting_welcome_description_edit': handle_adm_welcome_description_edit_message, # <<< ADDED
        'awaiting_welcome_confirmation': None, # Handled by callback (confirm_save_welcome)
        # ----------------------------
        # --- Refill ---
        'awaiting_refill_amount': handle_refill_amount_message,
        'awaiting_refill_crypto_choice': None, # Handled by callback
        'awaiting_basket_crypto_choice': None, # Also handled by callback
        # --- User Management States ---
        'awaiting_balance_adjustment_amount': handle_adjust_balance_amount_message,
        'awaiting_balance_adjustment_reason': handle_adjust_balance_reason_message,
        # ----------------------------
        # <<< Reseller Management States >>> # <<< ADDED
        'awaiting_reseller_manage_id': handle_reseller_manage_id_message,
        'awaiting_reseller_discount_percent': handle_reseller_percent_message,
        # -------------------------------- # <<< END ADDED
    }

    handler_func = STATE_HANDLERS.get(state)
    if handler_func:
        await handler_func(update, context)
    else:
        # Check if user is banned before processing other messages
        if state is None: # Only check if not in a specific state
            conn = None
            is_banned = False
            try:
                conn = get_db_connection()
                c = conn.cursor()
                c.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,))
                res = c.fetchone()
                if res and res['is_banned'] == 1:
                    is_banned = True
            except sqlite3.Error as e:
                logger.error(f"DB error checking ban status for user {user_id}: {e}")
            finally:
                if conn: conn.close()

            if is_banned:
                logger.info(f"Ignoring message from banned user {user_id}.")
                return # Don't process commands/messages from banned users

        logger.debug(f"Ignoring message from user {user_id} in state: {state}")

# --- Error Handler ---
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Logs errors caused by Updates."""
    logger.error(msg="Exception while handling an update:", exc_info=context.error)
    # Add logging for the error type itself
    logger.error(f"Caught error type: {type(context.error)}")
    chat_id = None
    user_id = None # Added to potentially identify user in logs

    if isinstance(update, Update):
        if update.effective_chat:
            chat_id = update.effective_chat.id
        if update.effective_user:
            user_id = update.effective_user.id

    # Log context details for better debugging
    logger.debug(f"Error context: user_data={context.user_data}, chat_data={context.chat_data}")

    # Don't send error messages for webhook-related processing errors
    if chat_id:
        error_message = "An internal error occurred. Please try again later or contact support."
        # *** FIXED: Use imported specific error classes ***
        if isinstance(context.error, BadRequest):
            if "message is not modified" in str(context.error).lower():
                logger.debug(f"Ignoring 'message is not modified' error for chat {chat_id}.")
                return # Don't notify user for this specific error
            logger.warning(f"Telegram API BadRequest for chat {chat_id} (User: {user_id}): {context.error}")
            if "can't parse entities" in str(context.error).lower():
                error_message = "An error occurred displaying the message due to formatting. Please try again."
            else:
                 error_message = "An error occurred communicating with Telegram. Please try again."
        elif isinstance(context.error, NetworkError):
            logger.warning(f"Telegram API NetworkError for chat {chat_id} (User: {user_id}): {context.error}")
            error_message = "A network error occurred. Please check your connection and try again."
        elif isinstance(context.error, Forbidden): # <-- FIXED: Use Forbidden for blocked/kicked
             logger.warning(f"Forbidden error for chat {chat_id} (User: {user_id}): Bot possibly blocked or kicked.")
             # Don't try to send a message if blocked
             return
        elif isinstance(context.error, RetryAfter): # <-- Handle RetryAfter
             retry_seconds = context.error.retry_after + 1
             logger.warning(f"Rate limit hit during update processing for chat {chat_id}. Error: {context.error}")
             # Don't send a message back for rate limit errors in handler
             return
        elif isinstance(context.error, sqlite3.Error):
            logger.error(f"Database error during update handling for chat {chat_id} (User: {user_id}): {context.error}", exc_info=True)
            # Don't expose detailed DB errors to the user
        # Handle potential job queue errors (like the NameError we saw before)
        elif isinstance(context.error, NameError):
             logger.error(f"NameError encountered for chat {chat_id} (User: {user_id}): {context.error}", exc_info=True)
             # Check if it's the one we identified
             if 'clear_expired_basket' in str(context.error):
                 logger.error("Error likely due to missing import in payment.py.")
                 error_message = "An internal processing error occurred (payment). Please try again."
             else:
                 error_message = "An internal processing error occurred. Please try again or contact support if it persists."
        elif isinstance(context.error, AttributeError): # Catch the specific AttributeError
             logger.error(f"AttributeError encountered for chat {chat_id} (User: {user_id}): {context.error}", exc_info=True)
             # Check if it's the one we identified for job context
             if "'NoneType' object has no attribute 'get'" in str(context.error) and "_process_collected_media" in str(context.error.__traceback__):
                 logger.error("Error likely due to missing user_data in job context.")
                 error_message = "An internal processing error occurred (media group). Please try again."
             # Check if it's the one from the main webhook handler
             elif "'module' object has no attribute" in str(context.error) and "handle_confirm_pay" in str(context.error):
                 logger.critical(f"CRITICAL IMPORT ERROR: main.py cannot find handle_confirm_pay in payment.py. Check imports/function name.")
                 error_message = "A critical configuration error occurred. Please contact support immediately."
             else:
                 error_message = "An unexpected internal error occurred. Please contact support."
        else:
             logger.exception(f"An unexpected error occurred during update handling for chat {chat_id} (User: {user_id}).")
             error_message = "An unexpected error occurred. Please contact support."

        # Attempt to send error message to the user
        try:
            # Use the application instance stored globally if context.bot is not available
            bot_instance = context.bot if hasattr(context, 'bot') else (telegram_app.bot if telegram_app else None)
            if bot_instance:
                 # Use send_message_with_retry for resilience
                 await send_message_with_retry(bot_instance, chat_id, error_message, parse_mode=None)
            else:
                 logger.error("Could not get bot instance to send error message.")
        except Exception as e:
            logger.error(f"Failed to send error message to user {chat_id}: {e}")

# --- Bot Setup Functions ---
async def post_init(application: Application) -> None:
    """Post-initialization tasks, e.g., setting commands."""
    logger.info("Running post_init setup...")
    logger.info("Setting bot commands...")
    await application.bot.set_my_commands([
        BotCommand("start", "Start the bot / Main menu"),
        BotCommand("admin", "Access admin panel (Admin only)"),
    ])
    logger.info("Post_init finished.")

async def post_shutdown(application: Application) -> None:
    """Tasks to run on graceful shutdown."""
    logger.info("Running post_shutdown cleanup...")
    # No crypto client to close anymore
    logger.info("Post_shutdown finished.")

# Background Job Wrapper for Basket Clearing
async def clear_expired_baskets_job_wrapper(context: ContextTypes.DEFAULT_TYPE):
    """Wrapper to call the synchronous clear_all_expired_baskets."""
    logger.debug("Running background job: clear_expired_baskets_job")
    try:
        # Run the synchronous DB operation in a separate thread
        await asyncio.to_thread(clear_all_expired_baskets)
        logger.info("Background job: Cleared expired baskets.")
    except Exception as e:
        logger.error(f"Error in background job clear_expired_baskets_job: {e}", exc_info=True)


# --- Flask Webhook Routes ---

# *** NEW: Helper function for webhook verification ***
def verify_nowpayments_signature(request_data, signature_header, secret_key):
    """Verifies the signature provided by NOWPayments."""
    if not secret_key or not signature_header:
        logger.warning("IPN Secret Key or signature header missing. Cannot verify webhook.")
        return False

    try:
        raw_body = request.get_data()
        ordered_data = json.dumps(json.loads(raw_body), sort_keys=True)
        hmac_hash = hmac.new(secret_key.encode('utf-8'), ordered_data.encode('utf-8'), hashlib.sha512).hexdigest()
        logger.debug(f"Calculated HMAC: {hmac_hash}")
        logger.debug(f"Received Signature: {signature_header}")
        return hmac.compare_digest(hmac_hash, signature_header)
    except Exception as e:
        logger.error(f"Error during signature verification: {e}", exc_info=True)
        return False


# --- MODIFIED Webhook Handler ---
@flask_app.route("/webhook", methods=['POST'])
def nowpayments_webhook():
    """Handles Instant Payment Notifications (IPN) from NOWPayments."""
    global telegram_app, main_loop, NOWPAYMENTS_IPN_SECRET

    if not telegram_app or not main_loop:
        logger.error("Webhook received but Telegram app or event loop not initialized.")
        return Response(status=503)

    # --- SIGNATURE VERIFICATION (Disabled for testing) ---
    # signature = request.headers.get('x-nowpayments-sig')
    # if not verify_nowpayments_signature(request, signature, NOWPAYMENTS_IPN_SECRET):
    #     logger.error("Invalid NOWPayments webhook signature received or verification failed.")
    #     return Response("Invalid Signature", status=401)
    # logger.info("NOWPayments webhook signature verified.")
    logger.warning("!!! NOWPayments signature verification is temporarily disabled !!!") # Indicate verification is OFF
    # ------------------------------------------------------

    if not request.is_json:
        logger.warning("Webhook received non-JSON request.")
        return Response("Invalid Request", status=400)

    data = request.get_json()
    logger.info(f"NOWPayments IPN received (VERIFICATION DISABLED): {json.dumps(data)}") # Log indicates disabled

    required_keys = ['payment_id', 'payment_status', 'pay_currency', 'actually_paid']
    if not all(key in data for key in required_keys):
        logger.error(f"Webhook missing required keys (need 'actually_paid'). Data: {data}")
        return Response("Missing required keys", status=400)

    payment_id = data.get('payment_id')
    status = data.get('payment_status')
    pay_currency = data.get('pay_currency')
    actually_paid_str = data.get('actually_paid')
    parent_payment_id = data.get('parent_payment_id') # Check if it's a child payment

    # Ignore child payments for initial processing (overpayments/refunds handled separately if needed)
    if parent_payment_id:
         logger.info(f"Ignoring child payment webhook update {payment_id} (parent: {parent_payment_id}).")
         return Response("Child payment ignored", status=200)

    # --- Process 'finished', 'confirmed', OR 'partially_paid' status ---
    if status in ['finished', 'confirmed', 'partially_paid'] and actually_paid_str is not None:
        logger.info(f"Processing '{status}' payment: {payment_id}")
        try:
            actually_paid_decimal = Decimal(str(actually_paid_str))
            if actually_paid_decimal <= 0:
                logger.warning(f"Ignoring webhook for payment {payment_id} with zero or negative 'actually_paid': {actually_paid_decimal}")
                if status != 'confirmed': # Remove pending only if not confirmed yet (or failed/expired later)
                    asyncio.run_coroutine_threadsafe(asyncio.to_thread(remove_pending_deposit, payment_id, trigger="zero_paid"), main_loop)
                return Response("Zero amount paid", status=200)

            pending_info = asyncio.run_coroutine_threadsafe(
                asyncio.to_thread(get_pending_deposit, payment_id), main_loop
            ).result()

            if not pending_info:
                 logger.warning(f"Webhook Warning: Received update for payment ID {payment_id}, but no pending deposit found in DB.")
                 return Response("Pending deposit not found", status=200) # Acknowledge, but nothing to process

            user_id = pending_info['user_id']
            stored_currency = pending_info['currency']
            target_eur_decimal = Decimal(str(pending_info['target_eur_amount']))
            expected_crypto_decimal = Decimal(str(pending_info.get('expected_crypto_amount', '0.0')))
            is_purchase = pending_info.get('is_purchase') == 1
            basket_snapshot = pending_info.get('basket_snapshot') # Might be None
            discount_code_used = pending_info.get('discount_code_used') # Might be None
            log_prefix = "PURCHASE" if is_purchase else "REFILL"

            if stored_currency.lower() != pay_currency.lower():
                 logger.error(f"Currency mismatch for {log_prefix} {payment_id}. DB: {stored_currency}, Webhook: {pay_currency}")
                 asyncio.run_coroutine_threadsafe(asyncio.to_thread(remove_pending_deposit, payment_id, trigger="currency_mismatch"), main_loop)
                 return Response("Currency mismatch", status=400)

            # --- DIFFERENCE: Check if it's a purchase or refill ---
            if is_purchase:
                # --- Handle Purchase Finalization ---
                if expected_crypto_decimal > 0 and actually_paid_decimal < expected_crypto_decimal:
                    logger.warning(f"{log_prefix} {payment_id} UNDERPAID by user {user_id}. Expected {expected_crypto_decimal} {pay_currency}, received {actually_paid_decimal}. Purchase failed.")
                    lang_data_en = LANGUAGES.get('en', {})
                    fail_msg = lang_data_en.get("crypto_purchase_failed", "Payment Failed/Expired. Your items are no longer reserved.")
                    # Create a dummy context ONLY if telegram_app is available
                    dummy_context = ContextTypes.DEFAULT_TYPE(application=telegram_app, chat_id=user_id, user_id=user_id) if telegram_app else None
                    if dummy_context:
                        asyncio.run_coroutine_threadsafe(send_message_with_retry(telegram_app.bot, user_id, fail_msg, parse_mode=None), main_loop)
                    else:
                         logger.error("Cannot notify user of underpayment, telegram_app not ready.")
                    asyncio.run_coroutine_threadsafe(asyncio.to_thread(remove_pending_deposit, payment_id, trigger="failure"), main_loop)
                    return Response("Underpaid for purchase", status=200)

                logger.info(f"{log_prefix} {payment_id} SUFFICIENTLY PAID by user {user_id}. Finalizing purchase.")
                dummy_context = ContextTypes.DEFAULT_TYPE(application=telegram_app, chat_id=user_id, user_id=user_id) if telegram_app else None
                if not dummy_context:
                     logger.error(f"Cannot finalize purchase {payment_id}, telegram_app not ready.")
                     # CRITICAL: Payment received but cannot finalize. Leave pending record for manual check.
                     return Response("Internal error: App not ready", status=500)

                future = asyncio.run_coroutine_threadsafe(
                    payment.process_successful_crypto_purchase(user_id, basket_snapshot, discount_code_used, payment_id, dummy_context),
                    main_loop
                )
                try:
                    purchase_finalized = future.result(timeout=60)
                    if purchase_finalized:
                        asyncio.run_coroutine_threadsafe(asyncio.to_thread(remove_pending_deposit, payment_id, trigger="purchase_success"), main_loop)
                        logger.info(f"Successfully processed and removed pending record for {log_prefix} {payment_id}")
                    else:
                        logger.critical(f"CRITICAL: {log_prefix} {payment_id} paid, but process_successful_crypto_purchase FAILED for user {user_id}. Pending deposit NOT removed. Manual intervention required.")
                        if ADMIN_ID:
                           asyncio.run_coroutine_threadsafe(send_message_with_retry(telegram_app.bot, ADMIN_ID, f"⚠️ CRITICAL: Crypto purchase {payment_id} paid by user {user_id} but FAILED TO FINALIZE. Check logs!"), main_loop)
                except asyncio.TimeoutError:
                     logger.error(f"Timeout waiting for process_successful_crypto_purchase result for {payment_id}. Pending deposit NOT removed.")
                except Exception as e:
                     logger.error(f"Error getting result from process_successful_crypto_purchase for {payment_id}: {e}. Pending deposit NOT removed.", exc_info=True)

            else:
                # --- Handle Refill (Existing Logic) ---
                credited_eur_amount = Decimal('0.0')
                if expected_crypto_decimal > 0:
                    proportion = actually_paid_decimal / expected_crypto_decimal
                    credited_eur_amount = (proportion * target_eur_decimal)
                    logger.info(f"{log_prefix} {payment_id} ({status}): User {user_id} paid {actually_paid_decimal} / {expected_crypto_decimal} {pay_currency}. Crediting proportional {credited_eur_amount:.8f} EUR.")
                else:
                    logger.error(f"{log_prefix} {payment_id} ({status}): Could not calculate proportional credit for user {user_id} (expected amount zero). Crediting 0 EUR.")

                credited_eur_amount = (credited_eur_amount * FEE_ADJUSTMENT).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                logger.info(f"{log_prefix} {payment_id} ({status}): Final refill credit after fee/rounding: {credited_eur_amount:.2f} EUR.")

                if credited_eur_amount > 0:
                    dummy_context = ContextTypes.DEFAULT_TYPE(application=telegram_app, chat_id=user_id, user_id=user_id) if telegram_app else None
                    if not dummy_context:
                         logger.error(f"Cannot process refill {payment_id}, telegram_app not ready.")
                         # CRITICAL: Payment received but cannot add balance. Leave pending record.
                         return Response("Internal error: App not ready", status=500)

                    future = asyncio.run_coroutine_threadsafe(
                        payment.process_successful_refill(user_id, credited_eur_amount, payment_id, dummy_context),
                        main_loop
                    )
                    try:
                         db_update_success = future.result(timeout=30)
                         if db_update_success:
                              asyncio.run_coroutine_threadsafe(asyncio.to_thread(remove_pending_deposit, payment_id, trigger="refill_success"), main_loop)
                              logger.info(f"Successfully processed and removed pending deposit {payment_id} (Status: {status})")
                         else:
                              logger.critical(f"CRITICAL: {log_prefix} {payment_id} ({status}) processed, but process_successful_refill FAILED for user {user_id}. Pending deposit NOT removed. Manual intervention required.")
                    except asyncio.TimeoutError:
                         logger.error(f"Timeout waiting for process_successful_refill result for {payment_id}. Pending deposit NOT removed.")
                    except Exception as e:
                         logger.error(f"Error getting result from process_successful_refill for {payment_id}: {e}. Pending deposit NOT removed.", exc_info=True)
                else:
                    logger.warning(f"{log_prefix} {payment_id} ({status}): Calculated credited EUR is zero for user {user_id}. Removing pending deposit without updating balance.")
                    asyncio.run_coroutine_threadsafe(asyncio.to_thread(remove_pending_deposit, payment_id, trigger="zero_credit"), main_loop)

        except (ValueError, TypeError) as e:
            logger.error(f"Webhook Error: Invalid number format in webhook data for {payment_id}. Error: {e}. Data: {data}")
        except Exception as e:
            logger.error(f"Webhook Error: Could not process payment update {payment_id}.", exc_info=True)

    # --- Process other statuses (failed, expired, etc.) ---
    elif status in ['failed', 'expired', 'refunded']:
        logger.warning(f"Payment {payment_id} has status '{status}'. Removing pending record.")
        # Get pending info to check if it was a purchase and notify user
        pending_info_for_removal = None
        try:
            pending_info_for_removal = asyncio.run_coroutine_threadsafe(
                 asyncio.to_thread(get_pending_deposit, payment_id), main_loop
            ).result(timeout=5)
        except Exception as e:
            logger.error(f"Error checking pending deposit for {payment_id} before removal/notification: {e}")

        # Remove pending deposit record from DB (this now also handles un-reserving items if it was a purchase)
        asyncio.run_coroutine_threadsafe(
            asyncio.to_thread(remove_pending_deposit, payment_id, trigger="failure" if status == 'failed' else "expiry"), # Pass trigger
            main_loop
        )

        # Notify user if possible
        if pending_info_for_removal and telegram_app:
            user_id = pending_info_for_removal['user_id']
            is_purchase_failure = pending_info_for_removal.get('is_purchase') == 1
            try:
                # Get user's language for notification
                conn_lang = None; user_lang = 'en'
                try:
                    conn_lang = get_db_connection()
                    c_lang = conn_lang.cursor()
                    c_lang.execute("SELECT language FROM users WHERE user_id = ?", (user_id,))
                    lang_res = c_lang.fetchone()
                    if lang_res and lang_res['language'] in LANGUAGES: user_lang = lang_res['language']
                except Exception as lang_e: logger.error(f"Failed to get lang for user {user_id} notify: {lang_e}")
                finally:
                     if conn_lang: conn_lang.close()

                lang_data_local = LANGUAGES.get(user_lang, LANGUAGES['en'])
                # Send different message for failed purchase vs failed refill
                if is_purchase_failure:
                     fail_msg = lang_data_local.get("crypto_purchase_failed", "Payment Failed/Expired. Your items are no longer reserved.")
                else:
                     fail_msg = lang_data_local.get("payment_cancelled_or_expired", "Payment Status: Your payment ({payment_id}) was cancelled or expired.").format(payment_id=payment_id)

                dummy_context = ContextTypes.DEFAULT_TYPE(application=telegram_app, chat_id=user_id, user_id=user_id)
                asyncio.run_coroutine_threadsafe(
                     send_message_with_retry(telegram_app.bot, user_id, fail_msg, parse_mode=None),
                     main_loop
                )
            except Exception as notify_e:
                 logger.error(f"Error notifying user {user_id} about failed/expired payment {payment_id}: {notify_e}")

    else:
         # Ignores 'waiting', 'confirming', 'sending', etc.
         logger.info(f"Webhook received for payment {payment_id} with status: {status} (ignored).")

    return Response(status=200) # Always acknowledge receipt


@flask_app.route(f"/telegram/{TOKEN}", methods=['POST'])
async def telegram_webhook():
    """Handles incoming Telegram updates via webhook."""
    global telegram_app, main_loop
    if not telegram_app or not main_loop:
        logger.error("Telegram webhook received but app/loop not ready.")
        return Response(status=503)
    try:
        update_data = request.get_json(force=True)
        update = Update.de_json(update_data, telegram_app.bot)
        # Process update in the bot's event loop
        asyncio.run_coroutine_threadsafe(telegram_app.process_update(update), main_loop)
        return Response(status=200)
    except json.JSONDecodeError:
        logger.error("Telegram webhook received invalid JSON.")
        return Response("Invalid JSON", status=400)
    except Exception as e:
        logger.error(f"Error processing Telegram webhook: {e}", exc_info=True)
        return Response("Internal Server Error", status=500)


# --- Main Function ---
def main() -> None:
    """Start the bot and the Flask webhook server."""
    global telegram_app, main_loop
    logger.info("Starting bot...")

    # --- Initialize Database and Load Data ---
    init_db()
    load_all_data()

    # --- Initialize Telegram Application ---
    defaults = Defaults(parse_mode=None, block=False) # Default to plain text
    app_builder = ApplicationBuilder().token(TOKEN).defaults(defaults).job_queue(JobQueue())

    # Add handlers
    app_builder.post_init(post_init)
    app_builder.post_shutdown(post_shutdown)
    application = app_builder.build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", handle_admin_menu))
    application.add_handler(CallbackQueryHandler(handle_callback_query))
    application.add_handler(MessageHandler(
        (filters.TEXT & ~filters.COMMAND) | filters.PHOTO | filters.VIDEO | filters.ANIMATION | filters.Document.ALL,
        handle_message
    ))
    application.add_error_handler(error_handler)

    telegram_app = application # Store application globally for webhook access
    main_loop = asyncio.get_event_loop() # Get the current event loop

    # --- Setup Background Job for Baskets ---
    if BASKET_TIMEOUT > 0:
        job_queue = application.job_queue
        if job_queue:
            logger.info(f"Setting up background job for expired baskets (interval: 60s)...")
            job_queue.run_repeating(
                 clear_expired_baskets_job_wrapper,
                 interval=timedelta(seconds=60),
                 first=timedelta(seconds=10),
                 name="clear_baskets"
            )
            logger.info("Background job setup complete.")
        else:
            logger.warning("Job Queue is not available. Basket clearing job skipped.")
    else:
        logger.warning("BASKET_TIMEOUT is not positive. Skipping background job setup.")

    # --- Webhook Setup & Server Start ---
    async def setup_webhooks_and_run():
        nonlocal application
        logger.info("Initializing application...")
        await application.initialize()

        logger.info(f"Setting Telegram webhook to: {WEBHOOK_URL}/telegram/{TOKEN}")
        if await application.bot.set_webhook(url=f"{WEBHOOK_URL}/telegram/{TOKEN}", allowed_updates=Update.ALL_TYPES):
            logger.info("Telegram webhook set successfully.")
        else:
            logger.error("Failed to set Telegram webhook.")
            return

        await application.start()
        logger.info("Telegram application started (webhook mode).")

        port = int(os.environ.get("PORT", 10000)) # Default to 10000 for Render
        flask_thread = threading.Thread(
            target=lambda: flask_app.run(host='0.0.0.0', port=port, debug=False),
            daemon=True
        )
        flask_thread.start()
        logger.info(f"Flask server started in a background thread on port {port}.")

        logger.info("Main thread entering keep-alive loop...")
        while True:
            await asyncio.sleep(3600)

    # --- Run the main async setup ---
    try:
        main_loop.run_until_complete(setup_webhooks_and_run())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutdown signal received.")
    except Exception as e:
        logger.critical(f"Critical error in main execution: {e}", exc_info=True)
    finally:
        logger.info("Initiating shutdown...")
        if telegram_app:
            logger.info("Stopping Telegram application...")
            if main_loop and main_loop.is_running():
                 main_loop.run_until_complete(telegram_app.stop())
                 main_loop.run_until_complete(telegram_app.shutdown())
            else:
                 asyncio.run(telegram_app.shutdown())
            logger.info("Telegram application stopped.")
        logger.info("Bot shutdown complete.")


if __name__ == '__main__':
    main()

# --- END OF FILE main.py ---
