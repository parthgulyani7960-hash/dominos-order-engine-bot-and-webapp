'''Utility to calculate order total payable amount.
Calculates subtotal plus flat service charge.
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
    """Calculate total payable and service charge.
    No coupon caps, newbie, or welcome coupons are applied during checkout.
    """
    from ..database import SystemConfig
    
    # Retrieve bot fee configuration (defaults to 5.0 if not configured)
    bot_fee_cfg = db.query(SystemConfig).filter(SystemConfig.key == "bot_fee").first()
    service_charge = float(bot_fee_cfg.value) if bot_fee_cfg else 5.0
    
    total_payable = round(subtotal + service_charge, 2)
    return total_payable, service_charge, None
