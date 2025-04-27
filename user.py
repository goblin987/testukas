# --- START OF FILE user.py ---

import sqlite3
import time
import logging
import asyncio
import os # Import os for path joining
from datetime import datetime, timezone
from collections import defaultdict, Counter
from decimal import Decimal # Use Decimal for financial calculations

# --- Telegram Imports ---
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.constants import ParseMode
from telegram import helpers
import telegram.error as telegram_error
# -------------------------

# Import from utils
from utils import (
    CITIES, DISTRICTS, PRODUCT_TYPES, THEMES, LANGUAGES, BOT_MEDIA, ADMIN_ID, BASKET_TIMEOUT, MIN_DEPOSIT_EUR,
    format_currency, get_progress_bar, send_message_with_retry, format_discount_value,
    clear_expired_basket, fetch_last_purchases, get_user_status, fetch_reviews,
    NOWPAYMENTS_API_KEY, # Check if NOWPayments is configured
    get_db_connection, MEDIA_DIR, # Import helper and MEDIA_DIR
    DEFAULT_PRODUCT_EMOJI # Import default emoji
)
import json # <<< Make sure json is imported
import payment # <<< Make sure payment module is imported


# Logging setup
logger = logging.getLogger(__name__)

# Emojis (Defaults/Placeholders)
EMOJI_CITY = "🏙️"
EMOJI_DISTRICT = "🏘️"
# EMOJI_PRODUCT = "💎" # No longer primary source
EMOJI_HERB = "🌿" # Keep for potential specific logic if needed
EMOJI_PRICE = "💰"
EMOJI_QUANTITY = "🔢"
EMOJI_BASKET = "🛒"
EMOJI_PROFILE = "👤"
EMOJI_REFILL = "💸"
EMOJI_REVIEW = "📝"
EMOJI_PRICELIST = "📋"
EMOJI_LANG = "🌐"
EMOJI_BACK = "⬅️"
EMOJI_HOME = "🏠"
EMOJI_SHOP = "🛍️"
EMOJI_DISCOUNT = "🏷️"


# --- Helper to get language data ---
def _get_lang_data(context: ContextTypes.DEFAULT_TYPE) -> tuple[str, dict]:
    """Gets the current language code and corresponding language data dictionary."""
    lang = context.user_data.get("lang", "en")
    # <<< ADDED LOGGING >>>
    logger.debug(f"_get_lang_data: Retrieved lang '{lang}' from context.user_data.")
    lang_data = LANGUAGES.get(lang, LANGUAGES['en'])
    if lang not in LANGUAGES:
        logger.warning(f"_get_lang_data: Language '{lang}' not found in LANGUAGES dict. Falling back to 'en'.")
        lang = 'en' # Ensure lang variable reflects the fallback
    # <<< ADDED LOGGING >>>
    # Log first few keys for debugging, limit length if too many keys
    keys_sample = list(lang_data.keys())[:5]
    logger.debug(f"_get_lang_data: Returning lang '{lang}' and lang_data keys sample: {keys_sample}...")
    return lang, lang_data

# --- Helper Function to Build Start Menu ---
def _build_start_menu_content(user_id: int, username: str, lang_data: dict, context: ContextTypes.DEFAULT_TYPE) -> tuple[str, InlineKeyboardMarkup]:
    """Builds the text and keyboard for the start menu using provided lang_data."""
    # <<< ADDED LOGGING >>>
    logger.debug(f"_build_start_menu_content: Building menu for user {user_id} with lang_data starting with welcome: '{lang_data.get('welcome', 'N/A')}'")

    balance, purchases, basket_count = Decimal('0.0'), 0, 0
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT balance, total_purchases FROM users WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        if result:
            balance = Decimal(str(result['balance']))
            purchases = result['total_purchases']

        # Ensure basket count is up-to-date (handles expiration implicitly if needed)
        # Note: clear_expired_basket itself is synchronous and modifies context
        clear_expired_basket(context, user_id)
        basket = context.user_data.get("basket", [])
        basket_count = len(basket)
        if not basket: context.user_data.pop('applied_discount', None)

    except sqlite3.Error as e:
        logger.error(f"Database error fetching data for start menu build (user {user_id}): {e}", exc_info=True)
    finally:
        if conn: conn.close()

    # Build Message Text using the PASSED lang_data
    status = get_user_status(purchases)
    balance_str = format_currency(balance)
    welcome_template = lang_data.get("welcome", "👋 Welcome, {username}!") # Use passed lang_data
    status_label = lang_data.get("status_label", "Status")
    balance_label = lang_data.get("balance_label", "Balance")
    purchases_label = lang_data.get("purchases_label", "Total Purchases")
    basket_label = lang_data.get("basket_label", "Basket Items")
    shopping_prompt = lang_data.get("shopping_prompt", "Start shopping or explore your options below.")
    refund_note = lang_data.get("refund_note", "Note: No refunds.")
    progress_bar_str = get_progress_bar(purchases)
    status_line = f"{EMOJI_PROFILE} {status_label}: {status} {progress_bar_str}"
    balance_line = f"{EMOJI_PRICE} {balance_label}: {balance_str} EUR"
    purchases_line = f"📦 {purchases_label}: {purchases}"
    basket_line = f"{EMOJI_BASKET} {basket_label}: {basket_count}"
    welcome_part = welcome_template.format(username=username)
    full_welcome = (
        f"{welcome_part}\n\n{status_line}\n{balance_line}\n"
        f"{purchases_line}\n{basket_line}\n\n{shopping_prompt}\n\n⚠️ {refund_note}"
    )

    # Build Keyboard using the PASSED lang_data
    shop_button_text = lang_data.get("shop_button", "Shop")
    profile_button_text = lang_data.get("profile_button", "Profile")
    top_up_button_text = lang_data.get("top_up_button", "Top Up")
    reviews_button_text = lang_data.get("reviews_button", "Reviews")
    price_list_button_text = lang_data.get("price_list_button", "Price List")
    language_button_text = lang_data.get("language_button", "Language")
    admin_button_text = lang_data.get("admin_button", "🔧 Admin Panel")
    keyboard = [
        [InlineKeyboardButton(f"{EMOJI_SHOP} {shop_button_text}", callback_data="shop")],
        [InlineKeyboardButton(f"{EMOJI_PROFILE} {profile_button_text}", callback_data="profile"),
         InlineKeyboardButton(f"{EMOJI_REFILL} {top_up_button_text}", callback_data="refill")],
        [InlineKeyboardButton(f"{EMOJI_REVIEW} {reviews_button_text}", callback_data="reviews"),
         InlineKeyboardButton(f"{EMOJI_PRICELIST} {price_list_button_text}", callback_data="price_list"),
         InlineKeyboardButton(f"{EMOJI_LANG} {language_button_text}", callback_data="language")]
    ]
    if user_id == ADMIN_ID:
        keyboard.insert(0, [InlineKeyboardButton(admin_button_text, callback_data="admin_menu")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    return full_welcome, reply_markup


# --- User Command Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command and the initial welcome message."""
    user = update.effective_user
    chat_id = update.effective_chat.id
    is_callback = update.callback_query is not None
    user_id = user.id
    username = user.username or user.first_name or f"User_{user_id}"

    # Send Bot Media (Only on direct /start, not callbacks)
    if not is_callback and BOT_MEDIA.get("type") and BOT_MEDIA.get("path"):
        media_path = BOT_MEDIA["path"]
        media_type = BOT_MEDIA["type"]
        logger.info(f"Attempting to send BOT_MEDIA: type={media_type}, path={media_path}")

        # Check if file exists using asyncio.to_thread
        if await asyncio.to_thread(os.path.exists, media_path):
            try:
                # --- FIX STARTS HERE ---
                # Pass the file path directly to the send_* methods
                if media_type == "photo":
                    await context.bot.send_photo(chat_id=chat_id, photo=media_path)
                elif media_type == "video":
                    await context.bot.send_video(chat_id=chat_id, video=media_path)
                elif media_type == "gif":
                    # Note: GIFs might be sent as animation or video depending on how they were saved.
                    # If saved as .mp4 (common for GIFs by bots), send_animation is usually correct.
                    # If saved as .gif, send_animation should also work.
                    await context.bot.send_animation(chat_id=chat_id, animation=media_path)
                else:
                    logger.warning(f"Unsupported BOT_MEDIA type for sending: {media_type}")
                # --- FIX ENDS HERE ---

            except telegram_error.TelegramError as e:
                # Catch potential errors during sending (e.g., file too large, network issue)
                logger.error(f"Error sending BOT_MEDIA ({media_path}): {e}", exc_info=True)
            except Exception as e:
                # Catch any other unexpected errors during sending
                logger.error(f"Unexpected error sending BOT_MEDIA ({media_path}): {e}", exc_info=True)
        else:
            logger.warning(f"BOT_MEDIA path {media_path} not found on disk when trying to send.")


    # Ensure user exists and language context is set
    lang = context.user_data.get("lang", None)
    if lang is None:
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            # Ensure user exists
            c.execute("""
                INSERT INTO users (user_id, username, language) VALUES (?, ?, 'en')
                ON CONFLICT(user_id) DO UPDATE SET username=excluded.username
            """, (user_id, username))
            # Get language
            c.execute("SELECT language FROM users WHERE user_id = ?", (user_id,))
            result = c.fetchone()
            db_lang = result['language'] if result else 'en'
            lang = db_lang if db_lang and db_lang in LANGUAGES else 'en'
            conn.commit()
            context.user_data["lang"] = lang # Store in context
            logger.info(f"start: Set language for user {user_id} to '{lang}' from DB/default.")
        except sqlite3.Error as e:
            logger.error(f"DB error ensuring user/language in start for {user_id}: {e}")
            lang = 'en' # Default on error
            context.user_data["lang"] = lang
            logger.warning(f"start: Defaulted language to 'en' for user {user_id} due to DB error.")
        finally:
            if conn: conn.close()
    else:
        logger.info(f"start: Using existing language '{lang}' from context for user {user_id}.")

    # Build and Send/Edit Menu
    lang, lang_data = _get_lang_data(context) # Get final language data again after ensuring it's set
    full_welcome, reply_markup = _build_start_menu_content(user_id, username, lang_data, context)

    if is_callback:
        query = update.callback_query
        try:
             # Only edit if message content or markup has changed
             if query.message and (query.message.text != full_welcome or query.message.reply_markup != reply_markup):
                  await query.edit_message_text(full_welcome, reply_markup=reply_markup, parse_mode=None)
             elif query: await query.answer() # Acknowledge if not modified
        except telegram_error.BadRequest as e:
              if "message is not modified" not in str(e).lower():
                  logger.warning(f"Failed to edit start message (callback): {e}. Sending new.")
                  await send_message_with_retry(context.bot, chat_id, full_welcome, reply_markup=reply_markup, parse_mode=None)
              elif query: await query.answer()
        except Exception as e:
             logger.error(f"Unexpected error editing start message (callback): {e}", exc_info=True)
             await send_message_with_retry(context.bot, chat_id, full_welcome, reply_markup=reply_markup, parse_mode=None)
    else:
        # Send the main welcome message *after* attempting to send the media
        await send_message_with_retry(context.bot, chat_id, full_welcome, reply_markup=reply_markup, parse_mode=None)


# --- Other handlers ---
async def handle_back_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles 'Back' button presses that should return to the main start menu."""
    await start(update, context)

# --- Shopping Handlers ---
async def handle_shop(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    logger.info(f"handle_shop triggered by user {user_id} (lang: {lang}).")

    no_cities_available_msg = lang_data.get("no_cities_available", "No cities available at the moment. Please check back later.")
    choose_city_title = lang_data.get("choose_city_title", "Choose a City")
    select_location_prompt = lang_data.get("select_location_prompt", "Select your location:")
    home_button_text = lang_data.get("home_button", "Home")

    if not CITIES:
        keyboard = [[InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]]
        await query.edit_message_text(f"{EMOJI_CITY} {no_cities_available_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        return

    try:
        sorted_city_ids = sorted(CITIES.keys(), key=lambda city_id: CITIES.get(city_id, ''))
        keyboard = []
        for c_id in sorted_city_ids:
             city_name = CITIES.get(c_id)
             if city_name: keyboard.append([InlineKeyboardButton(f"{EMOJI_CITY} {city_name}", callback_data=f"city|{c_id}")])
             else: logger.warning(f"handle_shop: City name missing for ID {c_id}.")
        keyboard.append([InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        message_text = f"{EMOJI_CITY} {choose_city_title}\n\n{select_location_prompt}"
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode=None)
        logger.info(f"handle_shop: Sent city list to user {user_id}.")
    except telegram_error.BadRequest as e:
         if "message is not modified" not in str(e).lower(): logger.error(f"Error editing shop message: {e}"); await query.answer("Error displaying cities.", show_alert=True)
         else: await query.answer()
    except Exception as e:
        logger.error(f"Error in handle_shop for user {user_id}: {e}", exc_info=True)
        try: keyboard = [[InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]]; await query.edit_message_text("❌ An error occurred.", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except Exception as inner_e: logger.error(f"Failed fallback in handle_shop: {inner_e}")


# --- Modified handle_city_selection ---
async def handle_city_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id # Added for logging
    lang, lang_data = _get_lang_data(context)

    if not params:
        logger.warning(f"handle_city_selection called without city_id for user {user_id}.")
        await query.answer("Error: City ID missing.", show_alert=True)
        return
    city_id = params[0]
    city_name = CITIES.get(city_id)
    if not city_name:
        error_city_not_found = lang_data.get("error_city_not_found", "Error: City not found.")
        logger.warning(f"City ID {city_id} not found in CITIES for user {user_id}.")
        await query.edit_message_text(f"❌ {error_city_not_found}", parse_mode=None)
        return await handle_shop(update, context) # Go back to city selection

    districts_in_city = DISTRICTS.get(city_id, {})
    back_cities_button = lang_data.get("back_cities_button", "Back to Cities")
    home_button = lang_data.get("home_button", "Home")
    no_districts_msg = lang_data.get("no_districts_available", "No districts available yet for this city.")
    no_products_in_districts_msg = lang_data.get("no_products_in_city_districts", "No products currently available in any district of this city.")
    choose_district_prompt = lang_data.get("choose_district_prompt", "Choose a district:")
    error_loading_districts = lang_data.get("error_loading_districts", "Error loading districts. Please try again.")
    available_label_short = lang_data.get("available_label_short", "Av") # Get short available label

    keyboard = []
    message_text_parts = [f"{EMOJI_CITY} {city_name}\n\n"] # Start message
    districts_with_products_info = [] # Store tuples: (d_id, dist_name)

    if not districts_in_city:
        # If no districts are configured AT ALL for the city
        keyboard_nav = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_cities_button}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
        await query.edit_message_text(f"{EMOJI_CITY} {city_name}\n\n{no_districts_msg}", reply_markup=InlineKeyboardMarkup(keyboard_nav), parse_mode=None)
        return
    else:
        # If districts are configured, check each one for products
        sorted_district_ids = sorted(districts_in_city.keys(), key=lambda dist_id: districts_in_city.get(dist_id, ''))
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()

            for d_id in sorted_district_ids:
                dist_name = districts_in_city.get(d_id)
                if dist_name:
                    # NEW Query for detailed product summary in this district
                    c.execute("""
                        SELECT product_type, size, price, COUNT(*) as quantity
                        FROM products
                        WHERE city = ? AND district = ? AND available > reserved
                        GROUP BY product_type, size, price
                        ORDER BY product_type, price, size
                    """, (city_name, dist_name))
                    products_in_district = c.fetchall()

                    if products_in_district:
                        # Add district header to message text
                        message_text_parts.append(f"{EMOJI_DISTRICT} **{dist_name}:**\n") # Make district bold
                        # Add product details to message text
                        for prod in products_in_district:
                            prod_emoji = PRODUCT_TYPES.get(prod['product_type'], DEFAULT_PRODUCT_EMOJI)
                            price_str = format_currency(prod['price'])
                            # Use plain text for product summary to avoid Markdown issues
                            message_text_parts.append(f"  • {prod_emoji} {prod['product_type']} {prod['size']} ({price_str}€) - {prod['quantity']} {available_label_short}\n")
                        message_text_parts.append("\n") # Add space after district info
                        # Add district to list for button creation
                        districts_with_products_info.append((d_id, dist_name))
                    # else: District has no products, do nothing (it's skipped)
                else:
                    logger.warning(f"District name missing for ID {d_id} in city {city_id} (handle_city_selection)")

        except sqlite3.Error as e:
            logger.error(f"DB error checking product availability for districts in city {city_name} (ID: {city_id}) for user {user_id}: {e}")
            keyboard_error = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_cities_button}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"{EMOJI_CITY} {city_name}\n\n❌ {error_loading_districts}", reply_markup=InlineKeyboardMarkup(keyboard_error), parse_mode=None)
            if conn: conn.close()
            return # Stop processing on DB error
        finally:
            if conn:
                conn.close()

        # After checking all districts:
        if not districts_with_products_info:
            # If we looped through all configured districts but none had products
            keyboard_nav = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_cities_button}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"{EMOJI_CITY} {city_name}\n\n{no_products_in_districts_msg}", reply_markup=InlineKeyboardMarkup(keyboard_nav), parse_mode=None)
        else:
            message_text_parts.append(f"\n{choose_district_prompt}") # Add prompt below details
            final_message = "".join(message_text_parts)
            # Create buttons ONLY for districts with products
            for d_id, dist_name in districts_with_products_info:
                 keyboard.append([InlineKeyboardButton(f"{EMOJI_DISTRICT} {dist_name}", callback_data=f"dist|{city_id}|{d_id}")])

            keyboard.append([InlineKeyboardButton(f"{EMOJI_BACK} {back_cities_button}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")])

            # Check length and edit message
            # Use parse_mode=None since we formatted manually and bold might fail
            try:
                if len(final_message) > 4000:
                    final_message = final_message[:4000] + "\n\n[... Message truncated ...]"
                    logger.warning(f"District selection message for user {user_id} city {city_name} truncated.")
                await query.edit_message_text(final_message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
            except telegram_error.BadRequest as e:
                if "message is not modified" not in str(e).lower():
                    logger.error(f"Error editing district selection message: {e}")
                    await query.answer("Error displaying districts.", show_alert=True)
                else:
                    await query.answer() # Acknowledge if not modified


async def handle_district_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    if not params or len(params) < 2: logger.warning("handle_district_selection missing params."); await query.answer("Error: City/District ID missing.", show_alert=True); return
    city_id, dist_id = params[0], params[1]
    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)

    if not city or not district: error_district_city_not_found = lang_data.get("error_district_city_not_found", "Error: District or city not found."); await query.edit_message_text(f"❌ {error_district_city_not_found}", parse_mode=None); return await handle_shop(update, context)

    back_districts_button = lang_data.get("back_districts_button", "Back to Districts"); home_button = lang_data.get("home_button", "Home")
    no_types_msg = lang_data.get("no_types_available", "No product types currently available here."); select_type_prompt = lang_data.get("select_type_prompt", "Select product type:")
    error_loading_types = lang_data.get("error_loading_types", "Error: Failed to Load Product Types"); error_unexpected = lang_data.get("error_unexpected", "An unexpected error occurred")

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT DISTINCT product_type FROM products WHERE city = ? AND district = ? AND available > reserved ORDER BY product_type", (city, district))
        available_types = [row['product_type'] for row in c.fetchall()]

        if not available_types:
            keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_districts_button}", callback_data=f"city|{city_id}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"{EMOJI_CITY} {city}\n{EMOJI_DISTRICT} {district}\n\n{no_types_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        else:
            keyboard = []
            for pt in available_types:
                emoji = PRODUCT_TYPES.get(pt, DEFAULT_PRODUCT_EMOJI)
                keyboard.append([InlineKeyboardButton(f"{emoji} {pt}", callback_data=f"type|{city_id}|{dist_id}|{pt}")])
            keyboard.append([InlineKeyboardButton(f"{EMOJI_BACK} {back_districts_button}", callback_data=f"city|{city_id}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")])
            await query.edit_message_text(f"{EMOJI_CITY} {city}\n{EMOJI_DISTRICT} {district}\n\n{select_type_prompt}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except sqlite3.Error as e: logger.error(f"DB error fetching product types {city}/{district}: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_loading_types}", parse_mode=None)
    except Exception as e: logger.error(f"Unexpected error in handle_district_selection: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_unexpected}", parse_mode=None)
    finally:
        if conn: conn.close()


async def handle_type_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    if not params or len(params) < 3: logger.warning("handle_type_selection missing params."); await query.answer("Error: City/District/Type missing.", show_alert=True); return
    city_id, dist_id, p_type = params
    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)

    if not city or not district: error_district_city_not_found = lang_data.get("error_district_city_not_found", "Error: District or city not found."); await query.edit_message_text(f"❌ {error_district_city_not_found}", parse_mode=None); return await handle_shop(update, context)

    product_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    back_types_button = lang_data.get("back_types_button", "Back to Types"); home_button = lang_data.get("home_button", "Home")
    no_items_of_type = lang_data.get("no_items_of_type", "No items of this type currently available here.")
    available_options_prompt = lang_data.get("available_options_prompt", "Available options:")
    error_loading_products = lang_data.get("error_loading_products", "Error: Failed to Load Products"); error_unexpected = lang_data.get("error_unexpected", "An unexpected error occurred")

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT size, price, COUNT(*) as count_available FROM products WHERE city = ? AND district = ? AND product_type = ? AND available > reserved GROUP BY size, price ORDER BY price", (city, district, p_type))
        products = c.fetchall()

        if not products:
            keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_types_button}", callback_data=f"dist|{city_id}|{dist_id}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"{EMOJI_CITY} {city}\n{EMOJI_DISTRICT} {district}\n{product_emoji} {p_type}\n\n{no_items_of_type}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        else:
            keyboard = []
            available_label_short = lang_data.get("available_label_short", "Av")
            for row in products:
                size, price, count = row['size'], Decimal(str(row['price'])), row['count_available']
                price_str_formatted = format_currency(price)
                price_str_callback = f"{price:.2f}"
                button_text = f"{product_emoji} {size} ({price_str_formatted}€) - {available_label_short}: {count}"
                callback_data = f"product|{city_id}|{dist_id}|{p_type}|{size}|{price_str_callback}"
                keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

            keyboard.append([InlineKeyboardButton(f"{EMOJI_BACK} {back_types_button}", callback_data=f"dist|{city_id}|{dist_id}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")])
            await query.edit_message_text(f"{EMOJI_CITY} {city}\n{EMOJI_DISTRICT} {district}\n{product_emoji} {p_type}\n\n{available_options_prompt}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except sqlite3.Error as e: logger.error(f"DB error fetching products {city}/{district}/{p_type}: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_loading_products}", parse_mode=None)
    except Exception as e: logger.error(f"Unexpected error in handle_type_selection: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_unexpected}", parse_mode=None)
    finally:
        if conn: conn.close()


async def handle_product_selection(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    if not params or len(params) < 5: logger.warning("handle_product_selection missing params."); await query.answer("Error: Incomplete product data.", show_alert=True); return
    city_id, dist_id, p_type, size, price_str = params

    try: price = Decimal(price_str)
    except ValueError: logger.warning(f"Invalid price format: {price_str}"); await query.edit_message_text("❌ Error: Invalid product data.", parse_mode=None); return

    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city or not district: error_location_mismatch = lang_data.get("error_location_mismatch", "Error: Location data mismatch."); await query.edit_message_text(f"❌ {error_location_mismatch}", parse_mode=None); return await handle_shop(update, context)

    product_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    theme_name = context.user_data.get("theme", "default")
    theme = THEMES.get(theme_name, THEMES["default"])
    basket_emoji = theme.get('basket', EMOJI_BASKET)

    price_label = lang_data.get("price_label", "Price"); available_label_long = lang_data.get("available_label_long", "Available")
    back_options_button = lang_data.get("back_options_button", "Back to Options"); home_button = lang_data.get("home_button", "Home")
    drop_unavailable_msg = lang_data.get("drop_unavailable", "Drop Unavailable! This option just sold out or was reserved.")
    add_to_basket_button = lang_data.get("add_to_basket_button", "Add to Basket")
    error_loading_details = lang_data.get("error_loading_details", "Error: Failed to Load Product Details"); error_unexpected = lang_data.get("error_unexpected", "An unexpected error occurred")

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT COUNT(*) as count FROM products WHERE city = ? AND district = ? AND product_type = ? AND size = ? AND price = ? AND available > reserved", (city, district, p_type, size, float(price)))
        available_count_result = c.fetchone(); available_count = available_count_result['count'] if available_count_result else 0

        if available_count <= 0:
            keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=f"type|{city_id}|{dist_id}|{p_type}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]
            await query.edit_message_text(f"❌ {drop_unavailable_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        else:
            price_formatted = format_currency(price)
            msg = (f"{EMOJI_CITY} {city} | {EMOJI_DISTRICT} {district}\n"
                   f"{product_emoji} {p_type} - {size}\n"
                   f"{EMOJI_PRICE} {price_label}: {price_formatted} EUR\n"
                   f"{EMOJI_QUANTITY} {available_label_long}: {available_count}")
            add_callback = f"add|{city_id}|{dist_id}|{p_type}|{size}|{price_str}"
            back_callback = f"type|{city_id}|{dist_id}|{p_type}"
            keyboard = [
                [InlineKeyboardButton(f"{basket_emoji} {add_to_basket_button}", callback_data=add_callback)],
                [InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=back_callback), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]
            ]
            await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    except sqlite3.Error as e: logger.error(f"DB error checking availability {city}/{district}/{p_type}/{size}: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_loading_details}", parse_mode=None)
    except Exception as e: logger.error(f"Unexpected error in handle_product_selection: {e}", exc_info=True); await query.edit_message_text(f"❌ {error_unexpected}", parse_mode=None)
    finally:
        if conn: conn.close()


async def handle_add_to_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    lang, lang_data = _get_lang_data(context)
    if not params or len(params) < 5: logger.warning("handle_add_to_basket missing params."); await query.answer("Error: Incomplete product data.", show_alert=True); return
    city_id, dist_id, p_type, size, price_str = params

    try: price = Decimal(price_str)
    except ValueError: logger.warning(f"Invalid price format add_to_basket: {price_str}"); await query.edit_message_text("❌ Error: Invalid product data.", parse_mode=None); return

    city = CITIES.get(city_id); district = DISTRICTS.get(city_id, {}).get(dist_id)
    if not city or not district: error_location_mismatch = lang_data.get("error_location_mismatch", "Error: Location data mismatch."); await query.edit_message_text(f"❌ {error_location_mismatch}", parse_mode=None); return await handle_shop(update, context)

    user_id = query.from_user.id
    product_emoji = PRODUCT_TYPES.get(p_type, DEFAULT_PRODUCT_EMOJI)
    theme_name = context.user_data.get("theme", "default"); theme = THEMES.get(theme_name, THEMES["default"])
    basket_emoji = theme.get('basket', EMOJI_BASKET)
    product_id_reserved = None; conn = None

    back_options_button = lang_data.get("back_options_button", "Back to Options"); home_button = lang_data.get("home_button", "Home")
    out_of_stock_msg = lang_data.get("out_of_stock", "Out of Stock! Sorry, the last one was taken or reserved.")
    pay_now_button_text = lang_data.get("pay_now_button", "Pay Now"); top_up_button_text = lang_data.get("top_up_button", "Top Up")
    view_basket_button_text = lang_data.get("view_basket_button", "View Basket"); clear_basket_button_text = lang_data.get("clear_basket_button", "Clear Basket")
    shop_more_button_text = lang_data.get("shop_more_button", "Shop More"); expires_label = lang_data.get("expires_label", "Expires")
    error_adding_db = lang_data.get("error_adding_db", "Error: Database issue adding item."); error_adding_unexpected = lang_data.get("error_adding_unexpected", "Error: An unexpected issue occurred.")
    added_msg_template = lang_data.get("added_to_basket", "✅ Item Reserved!\n\n{item} is in your basket for {timeout} minutes! ⏳")
    pay_msg_template = lang_data.get("pay", "💳 Total to Pay: {amount} EUR")
    apply_discount_button_text = lang_data.get("apply_discount_button", "Apply Discount Code")

    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("BEGIN EXCLUSIVE")
        c.execute("SELECT id FROM products WHERE city = ? AND district = ? AND product_type = ? AND size = ? AND price = ? AND available > reserved ORDER BY id LIMIT 1", (city, district, p_type, size, float(price)))
        product_row = c.fetchone()

        if not product_row:
            conn.rollback(); keyboard = [[InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=f"type|{city_id}|{dist_id}|{p_type}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]]; await query.edit_message_text(f"❌ {out_of_stock_msg}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None); return

        product_id_reserved = product_row['id']
        c.execute("UPDATE products SET reserved = reserved + 1 WHERE id = ?", (product_id_reserved,))
        c.execute("SELECT basket FROM users WHERE user_id = ?", (user_id,))
        user_basket_row = c.fetchone(); current_basket_str = user_basket_row['basket'] if user_basket_row else ''
        timestamp = time.time(); new_item_str = f"{product_id_reserved}:{timestamp}"
        new_basket_str = f"{current_basket_str},{new_item_str}" if current_basket_str else new_item_str
        c.execute("UPDATE users SET basket = ? WHERE user_id = ?", (new_basket_str, user_id))
        conn.commit()

        if "basket" not in context.user_data or not isinstance(context.user_data["basket"], list): context.user_data["basket"] = []
        context.user_data["basket"].append({"product_id": product_id_reserved, "price": price, "timestamp": timestamp})
        logger.info(f"User {user_id} added product {product_id_reserved} to basket.")

        timeout_minutes = BASKET_TIMEOUT // 60
        current_basket_list = context.user_data["basket"]

        original_total = sum(item['price'] for item in current_basket_list)
        final_total = original_total; discount_amount = Decimal('0.0')
        applied_discount_info = context.user_data.get('applied_discount')
        pay_msg_str = ""

        if applied_discount_info:
             code_valid, _, discount_details = validate_discount_code(applied_discount_info['code'], float(original_total))
             if code_valid and discount_details:
                 discount_amount = Decimal(str(discount_details['discount_amount']))
                 final_total = Decimal(str(discount_details['final_total']))
                 context.user_data['applied_discount']['amount'] = float(discount_amount)
                 context.user_data['applied_discount']['final_total'] = float(final_total)

        final_total_str = format_currency(final_total)
        pay_msg_str = pay_msg_template.format(amount=final_total_str)
        if discount_amount > 0:
             original_total_str = format_currency(original_total)
             discount_amount_str = format_currency(discount_amount)
             pay_msg_str = f"~{original_total_str} EUR~ - {discount_amount_str} EUR Discount\n{pay_msg_str}"

        item_price_str = format_currency(price)
        item_desc = f"{product_emoji} {p_type} {size} ({item_price_str}€)"
        expiry_dt = datetime.fromtimestamp(timestamp + BASKET_TIMEOUT); expiry_time_str = expiry_dt.strftime('%H:%M:%S')
        reserved_msg = (added_msg_template.format(timeout=timeout_minutes, item=item_desc) + "\n\n" + f"⏳ {expires_label}: {expiry_time_str}\n\n" + f"{pay_msg_str}")
        district_btn_text = district[:15]

        keyboard = [
            [InlineKeyboardButton(f"💳 {pay_now_button_text}", callback_data="confirm_pay"), InlineKeyboardButton(f"{EMOJI_REFILL} {top_up_button_text}", callback_data="refill")],
            [InlineKeyboardButton(f"{basket_emoji} {view_basket_button_text} ({len(current_basket_list)})", callback_data="view_basket"), InlineKeyboardButton(f"{basket_emoji} {clear_basket_button_text}", callback_data="clear_basket")],
            [InlineKeyboardButton(f"{EMOJI_DISCOUNT} {apply_discount_button_text}", callback_data="apply_discount_start")],
            [InlineKeyboardButton(f"➕ {shop_more_button_text} ({district_btn_text})", callback_data=f"dist|{city_id}|{dist_id}")],
            [InlineKeyboardButton(f"{EMOJI_BACK} {back_options_button}", callback_data=f"type|{city_id}|{dist_id}|{p_type}"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button}", callback_data="back_start")]
        ]
        await query.edit_message_text(reserved_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e:
        if conn and conn.in_transaction: conn.rollback()
        logger.error(f"DB error adding product {product_id_reserved if product_id_reserved else 'N/A'} user {user_id}: {e}", exc_info=True)
        await query.edit_message_text(f"❌ {error_adding_db}", parse_mode=None)
    except Exception as e:
        if conn and conn.in_transaction: conn.rollback()
        logger.error(f"Unexpected error adding item user {user_id}: {e}", exc_info=True)
        await query.edit_message_text(f"❌ {error_adding_unexpected}", parse_mode=None)
    finally:
        if conn: conn.close()


# --- Profile Handlers ---
# (handle_profile unchanged)
async def handle_profile(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    theme_name = context.user_data.get("theme", "default")
    theme = THEMES.get(theme_name, THEMES["default"])
    basket_emoji = theme.get('basket', EMOJI_BASKET)

    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT balance, total_purchases FROM users WHERE user_id = ?", (user_id,))
        result = c.fetchone()
        if not result: logger.error(f"User {user_id} not found in DB for profile."); await query.edit_message_text("❌ Error: Could not load profile.", parse_mode=None); return
        balance, purchases = Decimal(str(result['balance'])), result['total_purchases']

        clear_expired_basket(context, user_id)
        basket_count = len(context.user_data.get("basket", []))
        status = get_user_status(purchases); progress_bar = get_progress_bar(purchases); balance_str = format_currency(balance)
        status_label = lang_data.get("status_label", "Status"); balance_label = lang_data.get("balance_label", "Balance")
        purchases_label = lang_data.get("purchases_label", "Total Purchases"); basket_label = lang_data.get("basket_label", "Basket Items")
        profile_title = lang_data.get("profile_title", "Your Profile")
        profile_msg = (f"🎉 {profile_title}\n\n" f"👤 {status_label}: {status} {progress_bar}\n" f"💰 {balance_label}: {balance_str} EUR\n"
                       f"📦 {purchases_label}: {purchases}\n" f"🛒 {basket_label}: {basket_count}")

        top_up_button_text = lang_data.get("top_up_button", "Top Up"); view_basket_button_text = lang_data.get("view_basket_button", "View Basket")
        purchase_history_button_text = lang_data.get("purchase_history_button", "Purchase History"); home_button_text = lang_data.get("home_button", "Home")
        keyboard = [
            [InlineKeyboardButton(f"{EMOJI_REFILL} {top_up_button_text}", callback_data="refill"), InlineKeyboardButton(f"{basket_emoji} {view_basket_button_text} ({basket_count})", callback_data="view_basket")],
            [InlineKeyboardButton(f"📜 {purchase_history_button_text}", callback_data="view_history")],
            [InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]
        ]
        await query.edit_message_text(profile_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e: logger.error(f"DB error loading profile user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Failed to Load Profile.", parse_mode=None)
    except telegram_error.BadRequest as e:
        if "message is not modified" not in str(e).lower(): logger.error(f"Unexpected BadRequest handle_profile user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue.", parse_mode=None)
        else: await query.answer()
    except Exception as e: logger.error(f"Unexpected error handle_profile user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue.", parse_mode=None)
    finally:
        if conn: conn.close()

# --- Discount Validation (Synchronous) ---
# (validate_discount_code unchanged)
def validate_discount_code(code_text: str, current_total_float: float) -> tuple[bool, str, dict | None]:
    lang_data = LANGUAGES.get('en', {}) # Use English for internal messages
    no_code_msg = lang_data.get("no_code_provided", "No code provided.")
    not_found_msg = lang_data.get("discount_code_not_found", "Discount code not found.")
    inactive_msg = lang_data.get("discount_code_inactive", "This discount code is inactive.")
    expired_msg = lang_data.get("discount_code_expired", "This discount code has expired.")
    invalid_expiry_msg = lang_data.get("invalid_code_expiry_data", "Invalid code expiry data.")
    limit_reached_msg = lang_data.get("code_limit_reached", "Code reached usage limit.")
    internal_error_type_msg = lang_data.get("internal_error_discount_type", "Internal error processing discount type.")
    db_error_msg = lang_data.get("db_error_validating_code", "Database error validating code.")
    unexpected_error_msg = lang_data.get("unexpected_error_validating_code", "An unexpected error occurred.")
    code_applied_msg_template = lang_data.get("code_applied_message", "Code '{code}' ({value}) applied. Discount: -{amount} EUR")

    if not code_text: return False, no_code_msg, None
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("SELECT * FROM discount_codes WHERE code = ?", (code_text,))
        code_data = c.fetchone()

        if not code_data: return False, not_found_msg, None
        if not code_data['is_active']: return False, inactive_msg, None
        if code_data['expiry_date']:
            try:
                expiry_dt = datetime.fromisoformat(code_data['expiry_date'])
                if expiry_dt.tzinfo is None: expiry_dt = expiry_dt.astimezone()
                if datetime.now(expiry_dt.tzinfo) > expiry_dt: return False, expired_msg, None
            except ValueError: logger.warning(f"Invalid expiry_date format DB code {code_data['code']}"); return False, invalid_expiry_msg, None
        if code_data['max_uses'] is not None and code_data['uses_count'] >= code_data['max_uses']: return False, limit_reached_msg, None

        discount_amount = 0.0
        dtype = code_data['discount_type']; value = Decimal(str(code_data['value']))
        current_total_decimal = Decimal(str(current_total_float))

        if dtype == 'percentage': discount_amount = (current_total_decimal * value) / Decimal('100.0')
        elif dtype == 'fixed': discount_amount = value
        else: logger.error(f"Unknown discount type '{dtype}' code {code_data['code']}"); return False, internal_error_type_msg, None

        discount_amount = min(discount_amount, current_total_decimal)
        final_total_decimal = max(Decimal('0.0'), current_total_decimal - discount_amount)
        discount_amount_float = round(float(discount_amount), 2)
        final_total_float = round(float(final_total_decimal), 2)

        details = {'code': code_data['code'], 'type': dtype, 'value': float(value), 'discount_amount': discount_amount_float, 'final_total': final_total_float}
        code_display = code_data['code']; value_str_display = format_discount_value(dtype, float(value))
        amount_str_display = format_currency(discount_amount_float)
        message = code_applied_msg_template.format(code=code_display, value=value_str_display, amount=amount_str_display)
        return True, message, details

    except sqlite3.Error as e: logger.error(f"DB error validating discount code '{code_text}': {e}", exc_info=True); return False, db_error_msg, None
    except Exception as e: logger.error(f"Unexpected error validating code '{code_text}': {e}", exc_info=True); return False, unexpected_error_msg, None
    finally:
        if conn: conn.close()

# --- Basket Handlers ---
# (handle_view_basket unchanged)
async def handle_view_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    theme_name = context.user_data.get("theme", "default"); theme = THEMES.get(theme_name, THEMES["default"]); basket_emoji = theme.get('basket', EMOJI_BASKET)

    clear_expired_basket(context, user_id)
    basket = context.user_data.get("basket", [])
    applied_discount_info = context.user_data.get('applied_discount')
    discount_code_to_revalidate = applied_discount_info.get('code') if applied_discount_info else None

    if not basket:
        context.user_data.pop('applied_discount', None)
        basket_empty_msg = lang_data.get("basket_empty", "🛒 Your Basket is Empty!")
        add_items_prompt = lang_data.get("add_items_prompt", "Add items to start shopping!")
        shop_button_text = lang_data.get("shop_button", "Shop"); home_button_text = lang_data.get("home_button", "Home")
        full_empty_msg = basket_empty_msg + "\n\n" + add_items_prompt + " 😊"
        keyboard = [[InlineKeyboardButton(f"{EMOJI_SHOP} {shop_button_text}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]]
        try: await query.edit_message_text(full_empty_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except telegram_error.BadRequest as e:
             if "message is not modified" not in str(e).lower(): logger.error(f"Error editing empty basket msg: {e}")
             else: await query.answer()
        return

    msg = f"{basket_emoji} {lang_data.get('your_basket_title', 'Your Basket')}\n\n"
    original_total = Decimal('0.0')
    keyboard_items = []; product_db_details = {}; conn = None

    try:
        product_ids_in_basket = list(set(item['product_id'] for item in basket))
        if product_ids_in_basket:
             conn = get_db_connection()
             c = conn.cursor()
             placeholders = ','.join('?' for _ in product_ids_in_basket)
             c.execute(f"SELECT id, name, price, size, product_type FROM products WHERE id IN ({placeholders})", product_ids_in_basket)
             product_db_details = {row['id']: dict(row) for row in c.fetchall()}

        items_to_display_count = 0
        expires_in_label = lang_data.get("expires_in_label", "Expires in"); remove_button_label = lang_data.get("remove_button_label", "Remove")

        for index, item in enumerate(basket):
            prod_id = item['product_id']; details = product_db_details.get(prod_id)
            if not details: logger.warning(f"P{prod_id} missing DB details for view."); continue

            if 'price' in item and isinstance(item['price'], Decimal): price = item['price']
            else: price = Decimal(str(details['price']))

            timestamp = item['timestamp']
            product_type_name = details['product_type']
            product_emoji = PRODUCT_TYPES.get(product_type_name, DEFAULT_PRODUCT_EMOJI)
            item_desc = f"{product_emoji} {product_type_name} {details['size']}"
            item_price = format_currency(price)
            remaining_time = max(0, int(BASKET_TIMEOUT - (time.time() - timestamp)))
            time_str = f"{remaining_time // 60} min {remaining_time % 60} sec"
            msg += (f"{items_to_display_count + 1}. {item_desc} ({item_price}€)\n" f"   ⏳ {expires_in_label}: {time_str}\n")
            remove_button_text = f"🗑️ {remove_button_label} {item_desc}"[:60] # Truncate for safety
            keyboard_items.append([InlineKeyboardButton(remove_button_text, callback_data=f"remove|{prod_id}")])
            original_total += price
            items_to_display_count += 1

        if items_to_display_count == 0:
             context.user_data.pop('applied_discount', None); context.user_data['basket'] = []
             basket_empty_msg = lang_data.get("basket_empty", "🛒 Your Basket is Empty!"); items_expired_note = lang_data.get("items_expired_note", "Items may have expired or were removed.")
             shop_button_text = lang_data.get("shop_button", "Shop"); home_button_text = lang_data.get("home_button", "Home")
             full_empty_msg = basket_empty_msg + "\n\n" + items_expired_note
             keyboard = [[InlineKeyboardButton(f"🛍️ {shop_button_text}", callback_data="shop"), InlineKeyboardButton(f"🏠 {home_button_text}", callback_data="back_start")]]; await query.edit_message_text(full_empty_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None); return

        discount_amount = Decimal('0.0'); final_total = original_total; discount_applied_str = ""
        discount_applied_label = lang_data.get("discount_applied_label", "Discount Applied"); discount_removed_note_template = lang_data.get("discount_removed_note", "Discount code {code} removed: {reason}")

        if discount_code_to_revalidate:
            code_valid, validation_message, discount_details = validate_discount_code(discount_code_to_revalidate, float(original_total))
            if code_valid and discount_details:
                discount_amount = Decimal(str(discount_details['discount_amount']))
                final_total = Decimal(str(discount_details['final_total']))
                discount_code = discount_code_to_revalidate; discount_value = format_discount_value(discount_details['type'], discount_details['value'])
                discount_amount_str = format_currency(discount_amount)
                discount_applied_str = (f"\n{EMOJI_DISCOUNT} {discount_applied_label} ({discount_code}: {discount_value}): -{discount_amount_str} EUR")
                context.user_data['applied_discount'] = {'code': discount_code_to_revalidate, 'amount': float(discount_amount), 'final_total': float(final_total)}
            else:
                context.user_data.pop('applied_discount', None); logger.info(f"Discount '{discount_code_to_revalidate}' invalid user {user_id}. Reason: {validation_message}")
                discount_applied_str = f"\n{discount_removed_note_template.format(code=discount_code_to_revalidate, reason=validation_message)}"

        subtotal_label = lang_data.get("subtotal_label", "Subtotal"); total_label = lang_data.get("total_label", "Total")
        original_total_str = format_currency(original_total); final_total_str = format_currency(final_total)
        msg += f"\n{subtotal_label}: {original_total_str} EUR"
        msg += discount_applied_str if discount_applied_str else ""
        msg += f"\n💳 {total_label}: {final_total_str} EUR"

        pay_now_button_text = lang_data.get("pay_now_button", "Pay Now"); clear_all_button_text = lang_data.get("clear_all_button", "Clear All")
        remove_discount_button_text = lang_data.get("remove_discount_button", "Remove Discount"); apply_discount_button_text = lang_data.get("apply_discount_button", "Apply Discount Code")
        shop_more_button_text = lang_data.get("shop_more_button", "Shop More"); home_button_text = lang_data.get("home_button", "Home")

        action_buttons = [
            [InlineKeyboardButton(f"💳 {pay_now_button_text}", callback_data="confirm_pay"), InlineKeyboardButton(f"{basket_emoji} {clear_all_button_text}", callback_data="clear_basket")],
            *([[InlineKeyboardButton(f"❌ {remove_discount_button_text}", callback_data="remove_discount")]] if context.user_data.get('applied_discount') else []),
            [InlineKeyboardButton(f"{EMOJI_DISCOUNT} {apply_discount_button_text}", callback_data="apply_discount_start")],
            [InlineKeyboardButton(f"{EMOJI_SHOP} {shop_more_button_text}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]
        ]
        final_keyboard = keyboard_items + action_buttons

        try: await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(final_keyboard), parse_mode=None)
        except telegram_error.BadRequest as e:
             if "message is not modified" not in str(e).lower(): logger.error(f"Error editing basket view message: {e}")
             else: await query.answer()

    except sqlite3.Error as e: logger.error(f"DB error viewing basket user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Failed to Load Basket.", parse_mode=None)
    except Exception as e: logger.error(f"Unexpected error viewing basket user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue.", parse_mode=None)
    finally:
         if conn: conn.close()

# --- Discount Application Handlers ---
# (apply_discount_start unchanged)
async def apply_discount_start(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    clear_expired_basket(context, user_id)
    basket = context.user_data.get("basket", [])
    if not basket: no_items_message = lang_data.get("discount_no_items", "Your basket is empty."); await query.answer(no_items_message, show_alert=True); return await handle_view_basket(update, context)

    context.user_data['state'] = 'awaiting_user_discount_code'
    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="view_basket")]]
    enter_code_prompt = lang_data.get("enter_discount_code_prompt", "Please enter your discount code:")
    await query.edit_message_text(f"{EMOJI_DISCOUNT} {enter_code_prompt}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer(lang_data.get("enter_code_answer", "Enter code in chat."))

# (remove_discount unchanged)
async def remove_discount(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    if 'applied_discount' in context.user_data:
        removed_code = context.user_data.pop('applied_discount')['code']
        logger.info(f"User {user_id} removed discount code '{removed_code}'.")
        discount_removed_answer = lang_data.get("discount_removed_answer", "Discount removed.")
        await query.answer(discount_removed_answer)
    else: no_discount_answer = lang_data.get("no_discount_answer", "No discount applied."); await query.answer(no_discount_answer, show_alert=False)
    await handle_view_basket(update, context)

# (handle_user_discount_code_message unchanged)
async def handle_user_discount_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    state = context.user_data.get("state")
    lang, lang_data = _get_lang_data(context)

    if state != "awaiting_user_discount_code": return
    if not update.message or not update.message.text: send_text_please = lang_data.get("send_text_please", "Please send the code as text."); await send_message_with_retry(context.bot, chat_id, send_text_please, parse_mode=None); return

    entered_code = update.message.text.strip()
    context.user_data.pop('state', None)
    view_basket_button_text = lang_data.get("view_basket_button", "View Basket"); returning_to_basket_msg = lang_data.get("returning_to_basket", "Returning to basket.")

    if not entered_code: no_code_entered_msg = lang_data.get("no_code_entered", "No code entered."); await send_message_with_retry(context.bot, chat_id, no_code_entered_msg, parse_mode=None); keyboard = [[InlineKeyboardButton(view_basket_button_text, callback_data="view_basket")]]; await send_message_with_retry(context.bot, chat_id, returning_to_basket_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None); return

    clear_expired_basket(context, user_id)
    basket = context.user_data.get("basket", [])
    original_total_decimal = Decimal('0.0'); conn = None
    if basket:
         try:
            product_ids_in_basket = list(set(item['product_id'] for item in basket))
            conn = get_db_connection()
            c = conn.cursor()
            placeholders = ','.join('?' for _ in product_ids_in_basket)
            c.execute(f"SELECT id, price FROM products WHERE id IN ({placeholders})", product_ids_in_basket)
            prices_dict = {row['id']: Decimal(str(row['price'])) for row in c.fetchall()}
            original_total_decimal = sum(prices_dict.get(item['product_id'], Decimal('0.0')) for item in basket if item['product_id'] in prices_dict)
         except sqlite3.Error as e: logger.error(f"DB error recalculating total user {user_id}: {e}"); error_calc_total = lang_data.get("error_calculating_total", "Error calculating total."); await send_message_with_retry(context.bot, chat_id, f"❌ {error_calc_total}", parse_mode=None); kb = [[InlineKeyboardButton(view_basket_button_text, callback_data="view_basket")]]; await send_message_with_retry(context.bot, chat_id, returning_to_basket_msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None); return
         finally:
              if conn: conn.close()
    else:
        basket_empty_no_discount = lang_data.get("basket_empty_no_discount", "Basket empty. Cannot apply code."); await send_message_with_retry(context.bot, chat_id, basket_empty_no_discount, parse_mode=None); kb = [[InlineKeyboardButton(view_basket_button_text, callback_data="view_basket")]]; await send_message_with_retry(context.bot, chat_id, returning_to_basket_msg, reply_markup=InlineKeyboardMarkup(kb), parse_mode=None); return

    # Validation message is English
    code_valid, validation_message, discount_details = validate_discount_code(entered_code, float(original_total_decimal))

    if code_valid and discount_details:
        context.user_data['applied_discount'] = {'code': entered_code, 'amount': discount_details['discount_amount'], 'final_total': discount_details['final_total']}
        logger.info(f"User {user_id} applied discount code '{entered_code}'.")
        success_label = lang_data.get("success_label", "Success!")
        feedback_msg = f"✅ {success_label} {validation_message}"
    else:
        context.user_data.pop('applied_discount', None)
        logger.warning(f"User {user_id} failed to apply code '{entered_code}': {validation_message}")
        feedback_msg = f"❌ {validation_message}"

    keyboard = [[InlineKeyboardButton(view_basket_button_text, callback_data="view_basket")]]
    await send_message_with_retry(context.bot, chat_id, feedback_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)


# --- Remove From Basket ---
# (handle_remove_from_basket unchanged)
async def handle_remove_from_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    if not params: logger.warning(f"handle_remove_from_basket no product_id user {user_id}."); await query.answer("Error: Product ID missing.", show_alert=True); return
    try: product_id_to_remove = int(params[0])
    except ValueError: logger.warning(f"Invalid product_id format user {user_id}: {params[0]}"); await query.answer("Error: Invalid product data.", show_alert=True); return

    logger.info(f"Attempting remove product {product_id_to_remove} user {user_id}.")
    item_removed_from_context = False; item_to_remove_str = None; conn = None
    current_basket_context = context.user_data.get("basket", []); new_basket_context = []
    found_item_index = -1

    for index, item in enumerate(current_basket_context):
        if item.get('product_id') == product_id_to_remove:
            found_item_index = index
            try: timestamp_float = float(item['timestamp']); item_to_remove_str = f"{item['product_id']}:{timestamp_float}"
            except (ValueError, TypeError, KeyError) as e: logger.error(f"Invalid format in context item {item}: {e}"); item_to_remove_str = None
            break

    if found_item_index != -1:
        item_removed_from_context = True
        new_basket_context = current_basket_context[:found_item_index] + current_basket_context[found_item_index+1:]
        logger.debug(f"Found item {product_id_to_remove} in context user {user_id}. DB String: {item_to_remove_str}")
    else: logger.warning(f"Product {product_id_to_remove} not in user_data basket user {user_id}."); new_basket_context = list(current_basket_context)

    try:
        conn = get_db_connection()
        c = conn.cursor(); c.execute("BEGIN")
        if item_removed_from_context:
             update_result = c.execute("UPDATE products SET reserved = MAX(0, reserved - 1) WHERE id = ?", (product_id_to_remove,))
             if update_result.rowcount > 0: logger.debug(f"Decremented reservation P{product_id_to_remove}.")
             else: logger.warning(f"Could not find P{product_id_to_remove} to decrement reservation.")
        c.execute("SELECT basket FROM users WHERE user_id = ?", (user_id,))
        db_basket_result = c.fetchone(); db_basket_str = db_basket_result['basket'] if db_basket_result else ''
        if db_basket_str and item_to_remove_str:
            items_list = db_basket_str.split(',')
            if item_to_remove_str in items_list:
                items_list.remove(item_to_remove_str); new_db_basket_str = ','.join(items_list)
                c.execute("UPDATE users SET basket = ? WHERE user_id = ?", (new_db_basket_str, user_id)); logger.debug(f"Updated DB basket user {user_id} to: {new_db_basket_str}")
            else: logger.warning(f"Item string '{item_to_remove_str}' not found in DB basket '{db_basket_str}' user {user_id}.")
        elif item_removed_from_context and not item_to_remove_str: logger.warning(f"Could not construct item string for DB removal P{product_id_to_remove}.")
        elif not item_removed_from_context: logger.debug(f"Item {product_id_to_remove} not in context, DB basket not modified.")
        conn.commit()
        logger.info(f"DB ops complete remove P{product_id_to_remove} user {user_id}.")

        context.user_data['basket'] = new_basket_context
        if not context.user_data['basket']: context.user_data.pop('applied_discount', None)
        elif context.user_data.get('applied_discount'):
             applied_discount_info = context.user_data['applied_discount']
             basket_total_after_removal = float(sum(item.get('price', Decimal('0.0')) for item in context.user_data['basket']))
             code_valid, _, _ = validate_discount_code(applied_discount_info['code'], basket_total_after_removal)
             if not code_valid:
                 reason_removed = lang_data.get("discount_removed_invalid_basket", "Discount removed (basket changed).")
                 context.user_data.pop('applied_discount', None);
                 await query.answer(reason_removed, show_alert=False) # Notify user why it was removed

    except sqlite3.Error as e:
        if conn and conn.in_transaction: conn.rollback()
        logger.error(f"DB error removing item {product_id_to_remove} user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Failed to remove item (DB).", parse_mode=None); return
    except Exception as e:
        if conn and conn.in_transaction: conn.rollback()
        logger.error(f"Unexpected error removing item {product_id_to_remove} user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue removing item.", parse_mode=None); return
    finally:
        if conn: conn.close()
    await handle_view_basket(update, context)

# (handle_clear_basket unchanged)
async def handle_clear_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)
    conn = None

    current_basket_context = context.user_data.get("basket", [])
    if not current_basket_context: already_empty_msg = lang_data.get("basket_already_empty", "Basket already empty."); await query.answer(already_empty_msg, show_alert=False); return await handle_view_basket(update, context)

    product_ids_to_release_counts = Counter(item['product_id'] for item in current_basket_context)

    try:
        conn = get_db_connection()
        c = conn.cursor(); c.execute("BEGIN"); c.execute("UPDATE users SET basket = '' WHERE user_id = ?", (user_id,))
        if product_ids_to_release_counts:
             decrement_data = [(count, pid) for pid, count in product_ids_to_release_counts.items()]
             c.executemany("UPDATE products SET reserved = MAX(0, reserved - ?) WHERE id = ?", decrement_data)
             total_items_released = sum(product_ids_to_release_counts.values()); logger.info(f"Released {total_items_released} reservations user {user_id} clear.")
        conn.commit()
        context.user_data["basket"] = []; context.user_data.pop('applied_discount', None)
        logger.info(f"Cleared basket/discount user {user_id}.")
        shop_button_text = lang_data.get("shop_button", "Shop"); home_button_text = lang_data.get("home_button", "Home")
        cleared_msg = lang_data.get("basket_cleared", "🗑️ Basket Cleared!")
        keyboard = [[InlineKeyboardButton(f"{EMOJI_SHOP} {shop_button_text}", callback_data="shop"), InlineKeyboardButton(f"{EMOJI_HOME} {home_button_text}", callback_data="back_start")]]
        await query.edit_message_text(cleared_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)

    except sqlite3.Error as e:
        if conn and conn.in_transaction: conn.rollback()
        logger.error(f"DB error clearing basket user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: DB issue clearing basket.", parse_mode=None)
    except Exception as e:
        if conn and conn.in_transaction: conn.rollback()
        logger.error(f"Unexpected error clearing basket user {user_id}: {e}", exc_info=True); await query.edit_message_text("❌ Error: Unexpected issue.", parse_mode=None)
    finally:
        if conn: conn.close()


# --- Confirm Pay Handler (Modified for Insufficient Balance) ---
async def handle_confirm_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    """Handles the 'Pay Now' button press from the basket."""
    query = update.callback_query
    user_id = query.from_user.id
    chat_id = query.message.chat_id
    lang, lang_data = _get_lang_data(context)

    clear_expired_basket(context, user_id) # Sync call
    basket = context.user_data.get("basket", [])
    applied_discount_info = context.user_data.get('applied_discount')

    if not basket:
        await query.answer("Your basket is empty!", show_alert=True)
        await handle_view_basket(update, context) # Use await
        return

    # --- Variables to store results ---
    conn = None
    original_total = Decimal('0.0')
    final_total = Decimal('0.0')
    valid_basket_items_snapshot = []
    discount_code_to_use = None
    user_balance = Decimal('0.0')
    error_occurred = False # Flag

    # --- Fetch data and calculate ---
    try:
        conn = get_db_connection()
        c = conn.cursor()

        product_ids_in_basket = list(set(item['product_id'] for item in basket))
        if not product_ids_in_basket:
             await query.answer("Basket empty after validation.", show_alert=True)
             await handle_view_basket(update, context) # Use await
             return

        placeholders = ','.join('?' for _ in product_ids_in_basket)
        # Fetch necessary details for snapshot
        c.execute(f"SELECT id, price, name, size, product_type FROM products WHERE id IN ({placeholders})", product_ids_in_basket)
        prices_dict = {row['id']: dict(row) for row in c.fetchall()} # Store full dict

        for item in basket:
             prod_id = item['product_id']
             if prod_id in prices_dict:
                 db_price = Decimal(str(prices_dict[prod_id]['price']))
                 original_total += db_price
                 # Create snapshot with necessary info for later processing
                 item_snapshot = {
                     "product_id": prod_id,
                     "price": float(db_price), # Store as float for JSON later
                     "name": prices_dict[prod_id]['name'],
                     "size": prices_dict[prod_id]['size'],
                     "product_type": prices_dict[prod_id]['product_type']
                     # timestamp is not needed in snapshot for final processing
                 }
                 valid_basket_items_snapshot.append(item_snapshot)
             else: logger.warning(f"Product {prod_id} missing during payment confirm user {user_id}.")

        if not valid_basket_items_snapshot:
             context.user_data['basket'] = []
             context.user_data.pop('applied_discount', None)
             logger.warning(f"All items unavailable user {user_id} payment confirm.")
             keyboard_back = [[InlineKeyboardButton("⬅️ Back", callback_data="view_basket")]]
             try: await query.edit_message_text("❌ Error: All items unavailable.", reply_markup=InlineKeyboardMarkup(keyboard_back), parse_mode=None)
             except telegram_error.BadRequest: await send_message_with_retry(context.bot, chat_id, "❌ Error: All items unavailable.", reply_markup=InlineKeyboardMarkup(keyboard_back), parse_mode=None)
             return

        final_total = original_total
        if applied_discount_info:
            code_valid, _, discount_details = validate_discount_code(applied_discount_info['code'], float(original_total))
            if code_valid and discount_details:
                final_total = Decimal(str(discount_details['final_total']))
                discount_code_to_use = applied_discount_info.get('code')
                context.user_data['applied_discount']['final_total'] = float(final_total)
                context.user_data['applied_discount']['amount'] = discount_details['discount_amount']
            else:
                final_total = original_total
                discount_code_to_use = None
                context.user_data.pop('applied_discount', None)
                await query.answer("Applied discount became invalid.", show_alert=True)

        if final_total < Decimal('0.0'):
             await query.answer("Cannot process negative amount.", show_alert=True)
             return

        c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
        balance_result = c.fetchone()
        user_balance = Decimal(str(balance_result['balance'])) if balance_result else Decimal('0.0')

    except (sqlite3.Error, Exception) as e: # Catch potential errors here
        logger.error(f"Error during payment confirm data processing user {user_id}: {e}", exc_info=True)
        error_occurred = True # Set flag
        kb = [[InlineKeyboardButton("⬅️ Back", callback_data="view_basket")]]
        try: await query.edit_message_text("❌ Error preparing payment.", reply_markup=InlineKeyboardMarkup(kb), parse_mode=None)
        except Exception as edit_err: logger.error(f"Failed to edit message in error handler: {edit_err}")
    finally:
        if conn:
            conn.close() # Ensure connection is closed
            logger.debug("DB connection closed in handle_confirm_pay.")

    # --- Proceed only if no error occurred during data processing ---
    if error_occurred:
        return # Stop execution if an error happened

    # --- Balance Comparison and Action Logic ---
    logger.info(f"Payment confirm user {user_id}. Final Total: {final_total:.2f}, Balance: {user_balance:.2f}. Basket: {valid_basket_items_snapshot}")

    if user_balance >= final_total:
        # Pay with balance (Existing Logic - Calls helper which handles finalization)
        logger.info(f"Sufficient balance user {user_id}. Processing with balance.")
        try:
            if query.message: await query.edit_message_text("⏳ Processing payment with balance...", reply_markup=None, parse_mode=None)
            else: await send_message_with_retry(context.bot, chat_id, "⏳ Processing payment with balance...", parse_mode=None)
        except telegram_error.BadRequest: await query.answer("Processing...")

        # Pass the *snapshot* created earlier
        success = await payment.process_purchase_with_balance(user_id, final_total, valid_basket_items_snapshot, discount_code_to_use, context)

        if success:
            try:
                 if query.message: await query.edit_message_text("✅ Purchase successful! Details sent.", reply_markup=None, parse_mode=None)
            except telegram_error.BadRequest: pass # Ignore edit error after success
        # else: Failure message handled within process_purchase_with_balance

    else:
        # --- INSUFFICIENT BALANCE - NEW FLOW ---
        logger.info(f"Insufficient balance user {user_id}. Prompting for discount/crypto.")

        # Store necessary info for the next steps
        context.user_data['basket_pay_snapshot'] = valid_basket_items_snapshot
        context.user_data['basket_pay_total_eur'] = float(final_total)
        context.user_data['basket_pay_discount_code'] = discount_code_to_use # May be None

        needed_amount_str = format_currency(final_total)
        balance_str = format_currency(user_balance)

        insufficient_msg_template = lang_data.get("insufficient_balance_pay_option", "⚠️ Insufficient Balance! ({balance} / {required} EUR)")
        prompt_discount_msg = lang_data.get("prompt_discount_or_pay", "Do you have a discount code to apply before paying with crypto?")
        pay_crypto_button_text = lang_data.get("pay_crypto_button", "💳 Pay with Crypto")
        apply_discount_button_text = lang_data.get("apply_discount_pay_button", "🏷️ Apply Discount Code")
        back_basket_button_text = lang_data.get("back_basket_button", "Back to Basket")

        full_msg = (f"{insufficient_msg_template.format(balance=balance_str, required=needed_amount_str)}\n\n"
                   f"{prompt_discount_msg}")

        keyboard = [
            # Callback triggers asking for discount code
            [InlineKeyboardButton(apply_discount_button_text, callback_data="apply_discount_basket_pay")],
            # Callback skips discount and goes straight to crypto choice
            [InlineKeyboardButton(pay_crypto_button_text, callback_data="skip_discount_basket_pay")],
            [InlineKeyboardButton(f"⬅️ {back_basket_button_text}", callback_data="view_basket")]
        ]
        try:
            await query.edit_message_text(full_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        except telegram_error.BadRequest:
            await send_message_with_retry(context.bot, chat_id, full_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
        await query.answer() # Acknowledge button press

# --- NEW: Handler to Ask for Discount Code in Basket Pay Flow ---
async def handle_apply_discount_basket_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id
    lang, lang_data = _get_lang_data(context)

    # Check if context for this flow exists
    if 'basket_pay_snapshot' not in context.user_data or 'basket_pay_total_eur' not in context.user_data:
        logger.error(f"User {user_id} clicked apply_discount_basket_pay but context is missing.")
        await query.answer("Error: Context lost. Please go back to basket.", show_alert=True)
        return await handle_view_basket(update, context) # Go back to basket

    context.user_data['state'] = 'awaiting_basket_discount_code' # New state
    prompt_msg = lang_data.get("basket_pay_enter_discount", "Please enter discount code for this purchase:")
    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    # Cancel goes back to the discount/pay choice prompt
    keyboard = [[InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="confirm_pay")]] # Re-trigger confirm_pay to show options again

    await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=None)
    await query.answer("Enter discount code in chat.")

# --- NEW: Message Handler for Basket Pay Discount Code ---
async def handle_basket_discount_code_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    state = context.user_data.get("state")
    lang, lang_data = _get_lang_data(context)

    if state != "awaiting_basket_discount_code": return
    if not update.message or not update.message.text: return

    entered_code = update.message.text.strip()
    context.user_data.pop('state', None) # Clear state after getting code

    # Retrieve original total needed for validation
    basket_snapshot = context.user_data.get('basket_pay_snapshot')
    if not basket_snapshot:
        logger.error(f"User {user_id} sent basket discount code but snapshot context is missing.")
        await send_message_with_retry(context.bot, chat_id, "Error: Context lost. Returning to basket.", parse_mode=None)
        return await handle_view_basket(update, context)

    # Calculate original total from snapshot
    original_total_float = sum(item.get('price', 0.0) for item in basket_snapshot)

    if not entered_code:
        await send_message_with_retry(context.bot, chat_id, lang_data.get("no_code_entered", "No code entered."), parse_mode=None)
        # Re-show crypto choices without discount
        await _show_crypto_choices_for_basket(update, context)
        return

    code_valid, validation_message, discount_details = validate_discount_code(entered_code, original_total_float)

    feedback_msg_template = ""
    if code_valid and discount_details:
        new_final_total = discount_details['final_total']
        context.user_data['basket_pay_total_eur'] = new_final_total # Update the total to pay
        context.user_data['basket_pay_discount_code'] = entered_code # Store the code used
        logger.info(f"User {user_id} applied valid basket discount '{entered_code}'. New total: {new_final_total:.2f} EUR")
        feedback_msg_template = lang_data.get("basket_pay_code_applied", "✅ Code '{code}' applied. New total: {total} EUR. Choose crypto:")
        feedback_msg = feedback_msg_template.format(code=entered_code, total=format_currency(new_final_total))
    else:
        # Keep original total, don't store discount code
        context.user_data['basket_pay_discount_code'] = None
        logger.warning(f"User {user_id} entered invalid basket discount '{entered_code}': {validation_message}")
        original_total_eur = context.user_data.get('basket_pay_total_eur', 0.0) # Get potentially pre-discount total back
        feedback_msg_template = lang_data.get("basket_pay_code_invalid", "❌ Code invalid: {reason}. Choose crypto to pay {total} EUR:")
        feedback_msg = feedback_msg_template.format(reason=validation_message, total=format_currency(original_total_eur))

    # Delete the user's message containing the code
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=update.message.message_id)
    except Exception as e:
        logger.warning(f"Could not delete user's discount code message: {e}")

    # Send feedback and show crypto choices
    await send_message_with_retry(context.bot, chat_id, feedback_msg, parse_mode=None)
    await _show_crypto_choices_for_basket(update, context)

# --- NEW: Handler to Skip Discount in Basket Pay Flow ---
async def handle_skip_discount_basket_pay(update: Update, context: ContextTypes.DEFAULT_TYPE, params=None):
    query = update.callback_query
    user_id = query.from_user.id

    # Check if context for this flow exists
    if 'basket_pay_snapshot' not in context.user_data or 'basket_pay_total_eur' not in context.user_data:
        logger.error(f"User {user_id} clicked skip_discount_basket_pay but context is missing.")
        await query.answer("Error: Context lost. Please go back to basket.", show_alert=True)
        return await handle_view_basket(update, context) # Go back to basket

    context.user_data['basket_pay_discount_code'] = None # Ensure no discount code is stored
    await query.answer("Skipping discount...")
    # Edit the previous message to show crypto choices
    await _show_crypto_choices_for_basket(update, context, edit_message=True)


# --- NEW: Helper to Show Crypto Choices for Basket Payment ---
async def _show_crypto_choices_for_basket(update: Update, context: ContextTypes.DEFAULT_TYPE, edit_message: bool = False):
    """Displays cryptocurrency selection buttons for the basket total."""
    query = update.callback_query # May be None if called from message handler
    chat_id = update.effective_chat.id
    lang, lang_data = _get_lang_data(context)

    # Get stored total
    total_eur_float = context.user_data.get('basket_pay_total_eur')
    if total_eur_float is None:
        logger.error("Cannot show crypto choices for basket: total EUR missing from context.")
        msg = "Error: Payment amount missing. Returning to basket."
        kb = [[InlineKeyboardButton("⬅️ Back", callback_data="view_basket")]]
        if query and edit_message: await query.edit_message_text(msg, reply_markup=InlineKeyboardMarkup(kb))
        else: await send_message_with_retry(context.bot, chat_id, msg, reply_markup=InlineKeyboardMarkup(kb))
        if query: await query.answer("Error: Amount missing.", show_alert=True)
        return

    # Use same currencies as refill
    supported_currencies = {
        'BTC': 'btc', 'LTC': 'ltc', 'ETH': 'eth', 'SOL': 'sol',
        'USDT': 'usdt', 'USDC': 'usdc', 'TON': 'ton'
    }
    asset_buttons = []
    row = []
    for display, code in supported_currencies.items():
        # NEW Callback Data for basket payment crypto selection
        row.append(InlineKeyboardButton(display, callback_data=f"select_basket_crypto|{code}"))
        if len(row) >= 3:
            asset_buttons.append(row)
            row = []
    if row:
        asset_buttons.append(row)

    cancel_button_text = lang_data.get("cancel_button", "Cancel")
    asset_buttons.append([InlineKeyboardButton(f"❌ {cancel_button_text}", callback_data="view_basket")]) # Cancel returns to basket

    amount_str = format_currency(total_eur_float)
    prompt_template = lang_data.get("choose_crypto_for_purchase", "Choose crypto to pay {amount} EUR for your basket:")
    prompt_msg = prompt_template.format(amount=amount_str)

    if query and edit_message:
        try:
            await query.edit_message_text(prompt_msg, reply_markup=InlineKeyboardMarkup(asset_buttons), parse_mode=None)
        except telegram_error.BadRequest as e:
            if "message is not modified" not in str(e).lower():
                 logger.error(f"Error editing message for crypto choice (basket): {e}")
                 # Send as new message if edit fails
                 await send_message_with_retry(context.bot, chat_id, prompt_msg, reply_markup=InlineKeyboardMarkup(asset_buttons), parse_mode=None)
            elif query: await query.answer() # Acknowledge if not modified
    else:
        # Send as a new message if not editing (e.g., after discount code message)
        await send_message_with_retry(context.bot, chat_id, prompt_msg, reply_markup=InlineKeyboardMarkup(asset_buttons), parse_mode=None)

# --- END OF FILE user.py ---
