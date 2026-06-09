import random

## Simulation Engine: Handles math generation models for pricing, traffic, and supply profiles.

def get_traffic_and_supply(current_hour: float):
    """
    Phase 5: Traffic Patterns & Dynamic Supply/Demand
    Simulates peak rush hours, increasing traffic delays and dropping driver availability.
    """
    is_morning_rush = (8.0 <= current_hour <= 9.5)
    is_evening_rush = (17.0 <= current_hour <= 18.5)

    if is_morning_rush or is_evening_rush:
        traffic_multiplier = round(random.uniform(1.5, 2.5), 2)  # High delays
        demand_factor = random.randint(80, 100)                  # Massive passenger surge
        supply_factor = random.randint(10, 35)                   # Severe driver shortage
    else:
        traffic_multiplier = round(random.uniform(1.0, 1.2), 2)  # Minimal delays
        demand_factor = random.randint(20, 60)
        supply_factor = random.randint(50, 90)

    return traffic_multiplier, demand_factor, supply_factor

def calculate_surge(demand: int, supply: int) -> float:
    """
    Phase 5: Surge Pricing Logic
    Applies an exponential multiplier if demand vastly outpaces driver supply.
    """
    if supply == 0:
        return 3.0  # Cap maximum surge at 3.0x to prevent division by zero
    
    ratio = demand / supply
    
    if ratio > 2.5:
        return round(random.uniform(2.0, 3.0), 2)
    elif ratio > 1.5:
        return round(random.uniform(1.3, 1.9), 2)
    elif ratio > 1.0:
        return round(random.uniform(1.1, 1.25), 2)
    
    return 1.0  # No surge