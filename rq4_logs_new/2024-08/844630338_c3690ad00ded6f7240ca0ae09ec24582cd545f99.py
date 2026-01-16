class Crop:
    def __init__(self, name, soil_type, ideal_conditions, recommended_treatment):
        self.name = name
        self.soil_type = soil_type
        self.ideal_conditions = ideal_conditions
        self.recommended_treatment = recommended_treatment
crops = [
    Crop("Wheat", "Loamy", "Cool, dry climate", "Regular watering and weed control"),
    Crop("Rice", "Clay", "Warm, humid climate", "Frequent irrigation and pest control"),
    Crop("Maize", "Sandy", "Warm, dry climate", "Nutrient-rich soil and regular irrigation"),
    Crop("Potato", "Sandy loam", "Cool, moist climate", "Good drainage and pest control"),
    Crop("Soybean", "Clay loam", "Warm, moist climate", "Fertile soil with proper weed control")
]

def recommend_crop(soil_type):
    print(f"Recommended crops for {soil_type} soil:")
    for crop in crops:
        if crop.soil_type.lower() == soil_type.lower():
            print(f"Crop: {crop.name}, Ideal Conditions: {crop.ideal_conditions}, Treatment: {crop.recommended_treatment}")

def soil_management_advice(soil_type):
    advice = {
        "Loamy": "Loamy soil is fertile and well-drained. It requires regular organic matter addition.",
        "Clay": "Clay soil retains water but can be compact. Use organic matter and proper tillage.",
        "Sandy": "Sandy soil drains quickly and lacks nutrients. Add organic matter and nutrients regularly.",
        "Sandy loam": "Sandy loam is ideal for most crops. Ensure proper drainage and add organic matter.",
        "Clay loam": "Clay loam is fertile but needs good drainage. Use cover crops and mulch."
    }
    print(f"Soil management advice for {soil_type} soil:")
    print(advice.get(soil_type, "No advice available for this soil type."))

def main():
    print("Crop and Soil Management System")
    while True:
        print("\n1. Recommend a crop based on soil type")
        print("2. Get soil management advice")
        print("3. Exit")
        choice = input("Enter your choice: ")

        if choice == '1':
            soil_type = input("Enter your soil type, Choose from Loamy, Clay, Sandy, Sandy loam, Clay loam: ")
            recommend_crop(soil_type)
        
        elif choice == '2':
            soil_type = input("Enter your soil type, Choose from Loamy, Clay, Sandy, Sandy loam, Clay loam: ")
            soil_management_advice(soil_type)

        elif choice == '3':
            print("Exiting the system")
            break

        else:
            print("Invalid choice.Please try again.")

if __name__ == "__main__":
    main()