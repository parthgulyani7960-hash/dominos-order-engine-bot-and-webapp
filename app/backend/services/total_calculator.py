'''Utility to calculate order total payable amount.
Handles fixed pricing logic, bot fee, and location pricing multiplier.
'''

def calculate_total_payable(
    subtotal: float,
    val_min: float,
    val_max: float,
    val_fixed: float,
    discount_total: float,
    user,
    db,
    newbie_coupon_cfg,
    welcome_coupon_cfg,
    coupon_code: str = None
) -> tuple[float, float, str]:
    """Calculate total payable, bot fee using the fixed pricing model, adjusted by location multiplier.

    Returns:
        total_payable (float): final amount the user must pay.
        service_charge (float): the bot fee or service charge.
        coupon_applied (str): coupon code applied.
    """
    from ..database import SystemConfig, LocationPricing, Order
    
    # Get location multiplier if user has a city set
    multiplier = 1.0
    if user and user.city:
        loc = db.query(LocationPricing).filter(LocationPricing.city.ilike(user.city)).first()
        if loc:
            multiplier = loc.price_multiplier

    # Apply multiplier to fixed value
    fixed_price = round(val_fixed * multiplier, 2)

    newbie_val = newbie_coupon_cfg.value if hasattr(newbie_coupon_cfg, "value") else (newbie_coupon_cfg or "NEWBIE100")
    welcome_val = welcome_coupon_cfg.value if hasattr(welcome_coupon_cfg, "value") else (welcome_coupon_cfg or "WELCOME90")

    coupon_applied = None
    if val_min <= subtotal <= val_max:
        # Retrieve bot fee configuration (defaults to 10.0 if not configured)
        bot_fee_cfg = db.query(SystemConfig).filter(SystemConfig.key == "bot_fee").first()
        bot_fee = float(bot_fee_cfg.value) if bot_fee_cfg else 10.0
        
        total_payable = round(fixed_price + bot_fee, 2)
        service_charge = bot_fee
        
        # Check user's past order count (excluding Cancelled ones)
        if user:
            orders_count = db.query(Order).filter(Order.user_id == user.id, Order.status != "Cancelled").count()
            if orders_count == 0:
                coupon_applied = newbie_val
            else:
                coupon_applied = welcome_val
    else:
        # If order not within cap, service charge is 5.0
        total_payable = round(subtotal + 5.0, 2)
        service_charge = 5.0
        
    return total_payable, service_charge, coupon_applied
