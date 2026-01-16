def get_description_summary_prompt(description: str) -> str:
    return f"""
    Extract the following distinct information from this apartment description:
    
    1. Price (base monthly rent for the flat itself only)
    2. Deposit amount (kaucja in Polish) - this is a separate one-time security payment
    3. Whether pets/animals are allowed
    4. Additional rent or fees (czynsz in Polish) - these are separate recurring charges beyond the base rent
    
    Important rules:
    - Each value should be independent and not include other values (price should not include the rent fees)
    - Price refers ONLY to the base cost of renting the flat
    - Deposit is a separate one-time security payment
    - Rent fees are recurring monthly charges separate from the base price
    
    Currency conversion rules:
    - If amounts are in PLN (zł), keep the numeric value as is
    - If amounts are in USD ($) or EUR (€), multiply by 4 to convert to PLN
    - If amounts are in any other currency, use 0
    - If no currency is specified, assume PLN
    
    If any information is not provided in the description, use these defaults:
    - price: 0
    - deposit: 0
    - animals_allowed: NOT_SPECIFIED
    - rent: 0
    
    Apartment description:
    {description}
    
    IMPORTANT: Your response must ONLY contain these 4 lines with no additional text, introduction, or explanation:
    price: [integer in PLN]
    deposit: [integer in PLN]
    animals_allowed: [true/false/NOT_SPECIFIED]
    rent: [integer in PLN]
    
    Do not add any explanations, introductions, or conclusions. Output only these 4 lines exactly as specified.
    """