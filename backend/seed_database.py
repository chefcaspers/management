import random
import os
import sys
from flask import Flask
from models import db, Location, Kitchen, Vendor, Brand, Menu, Category, Item
import datetime

# Food-related data for realistic menu generation
CUISINES = [
    "Italian", "Mexican", "Chinese", "Japanese", "Thai", "Indian", "American", "Mediterranean", 
    "French", "Greek", "Spanish", "Vietnamese", "Korean", "Middle Eastern", "Caribbean"
]

FOOD_TYPES = [
    "Pizza", "Burger", "Sushi", "Taco", "Pasta", "Curry", "Sandwich", "Salad", 
    "Steak", "Seafood", "Vegetarian", "BBQ", "Breakfast", "Dessert", "Smoothie"
]

# Menu category names
CATEGORIES = [
    "Appetizers", "Main Courses", "Sides", "Desserts", "Beverages", 
    "Specials", "Combos", "Healthy Options", "Kids Menu", "Snacks"
]

# Menu items by category
MENU_ITEMS = {
    "Appetizers": [
        {"name": "Mozzarella Sticks", "description": "Crispy on the outside, melty on the inside", "price": 7.99},
        {"name": "Loaded Nachos", "description": "With cheese, jalapeños, and sour cream", "price": 9.99},
        {"name": "Buffalo Wings", "description": "Spicy chicken wings with blue cheese dip", "price": 10.99},
        {"name": "Garlic Bread", "description": "Toasted with garlic butter and herbs", "price": 5.99},
        {"name": "Spring Rolls", "description": "Vegetable filling with sweet chili sauce", "price": 6.99},
        {"name": "Stuffed Mushrooms", "description": "With herbed cream cheese", "price": 8.99},
        {"name": "Bruschetta", "description": "Tomatoes, basil, and balsamic on toast", "price": 7.49},
    ],
    "Main Courses": [
        {"name": "Classic Cheeseburger", "description": "Beef patty with cheese on a brioche bun", "price": 12.99},
        {"name": "Grilled Salmon", "description": "With lemon butter sauce and vegetables", "price": 18.99},
        {"name": "Chicken Alfredo", "description": "Creamy pasta with grilled chicken", "price": 15.99},
        {"name": "Margherita Pizza", "description": "Tomato sauce, mozzarella, and basil", "price": 14.99},
        {"name": "Beef Stir Fry", "description": "With vegetables and teriyaki sauce", "price": 16.99},
        {"name": "Vegetable Curry", "description": "Medium spiced with basmati rice", "price": 13.99},
        {"name": "Fish & Chips", "description": "Beer battered cod with fries", "price": 14.49},
    ],
    "Sides": [
        {"name": "French Fries", "description": "Classic crispy potato fries", "price": 3.99},
        {"name": "Onion Rings", "description": "Beer battered and crispy", "price": 4.99},
        {"name": "Side Salad", "description": "Mixed greens with house dressing", "price": 4.49},
        {"name": "Coleslaw", "description": "Creamy cabbage and carrot slaw", "price": 3.49},
        {"name": "Garlic Mashed Potatoes", "description": "Creamy potatoes with roasted garlic", "price": 4.99},
    ],
    "Desserts": [
        {"name": "Chocolate Cake", "description": "Rich and moist with chocolate ganache", "price": 6.99},
        {"name": "New York Cheesecake", "description": "Creamy with graham cracker crust", "price": 7.99},
        {"name": "Apple Pie", "description": "With cinnamon and vanilla ice cream", "price": 6.49},
        {"name": "Tiramisu", "description": "Coffee-soaked ladyfingers and mascarpone", "price": 7.49},
        {"name": "Ice Cream Sundae", "description": "Three scoops with toppings", "price": 5.99},
    ],
    "Beverages": [
        {"name": "Soft Drinks", "description": "Cola, lemon-lime, or root beer", "price": 2.99},
        {"name": "Fresh Lemonade", "description": "Homemade with real lemons", "price": 3.99},
        {"name": "Iced Tea", "description": "Sweet or unsweetened", "price": 2.99},
        {"name": "Coffee", "description": "Regular or decaf", "price": 2.49},
        {"name": "Milkshake", "description": "Chocolate, vanilla, or strawberry", "price": 5.99},
    ],
    "Specials": [
        {"name": "Chef's Special Pasta", "description": "Daily pasta creation with seasonal ingredients", "price": 17.99},
        {"name": "Surf & Turf", "description": "Steak and lobster tail", "price": 28.99},
        {"name": "Weekend Brunch Plate", "description": "Eggs, bacon, pancakes, and potatoes", "price": 14.99},
    ]
}

def create_app():
    """Create a Flask app instance for database operations"""
    app = Flask(__name__)
    
    # Use environment variables to decide between SQLite or Postgres
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        app.config['SQLALCHEMY_DATABASE_URI'] = db_url
    else:
        # Fall back to SQLite
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///ghost_kitchen.db'
    
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    db.init_app(app)
    
    return app

def generate_brand_name():
    """Generate a realistic brand name for a food establishment"""
    prefixes = ["Tasty", "Delicious", "Gourmet", "Fresh", "Royal", "Golden", "Silver", "Green", 
                "Blue", "Red", "Urban", "City", "Classic", "Modern", "Fusion", "Authentic"]
    
    suffixes = ["Kitchen", "Bistro", "Cafe", "Grill", "House", "Garden", "Table", "Plate", 
                "Spoon", "Fork", "Restaurant", "Diner", "Eatery", "Bites", "Flavors", "Taste"]
    
    cuisine_types = ["Italian", "Thai", "Mexican", "Chinese", "American", "BBQ", "Sushi", 
                    "Pizza", "Burger", "Salad", "Steak", "Seafood", "Vegan", "Fusion"]
    
    food_items = ["Pasta", "Noodle", "Taco", "Burger", "Pizza", "Sandwich", "Curry", 
                  "Rice", "Dumpling", "Pancake", "Waffle", "Grill", "BBQ"]
    
    name_types = [
        # Format: Prefix + Suffix
        lambda: f"{random.choice(prefixes)} {random.choice(suffixes)}",
        
        # Format: The + Food Item + Place
        lambda: f"The {random.choice(food_items)} {random.choice(suffixes)}",
        
        # Format: Name's + Suffix
        lambda: f"{random.choice(['Joe', 'Mary', 'Sam', 'Lucy', 'Tom', 'Anna', 'Max', 'Chef', 'Mama', 'Papa'])}'s {random.choice(suffixes)}",
        
        # Format: Cuisine + Suffix
        lambda: f"{random.choice(cuisine_types)} {random.choice(suffixes)}",
        
        # Format: Adjective + Food
        lambda: f"{random.choice(['Spicy', 'Sweet', 'Savory', 'Crispy', 'Hot', 'Wild', 'Hungry', 'Happy'])} {random.choice(food_items)}",
    ]
    
    return random.choice(name_types)()

def seed_database():
    """Populate the database with sample data"""
    try:
        print("Starting database seeding...")
        
        # Create locations
        locations = [
            Location(name="Los Angeles", address="1234 Sunset Blvd, Los Angeles, CA 90026, USA"),
            Location(name="London", address="47 Oxford Street, London W1D 2DW, United Kingdom"),
            Location(name="Singapore", address="75 Airport Blvd, Singapore 819664")
        ]
        
        for location in locations:
            db.session.add(location)
        
        db.session.commit()
        print(f"Created {len(locations)} locations")
        
        # Create vendors
        vendors = []
        vendor_names = [
            "Global Food Services", "Urban Eats Inc.", "Premier Kitchen Group",
            "Culinary Innovations", "Gourmet Holdings", "Flavor Ventures",
            "Foodie Enterprises", "Tasty Brands LLC", "Delicious Concepts",
            "Modern Meal Solutions", "Fresh Fare Co.", "Savory Selections",
            "Epicurean Ventures", "Cuisine Masters", "Meal Makers",
            "Kitchen Creations", "Food Fusion Group", "Dining Delights",
            "Taste Trends", "Culinary Crafters", "Gastronomy Group",
            "Plate Perfection", "Bite Brilliance", "Meal Mavericks",
            "Flavor Frontier", "Dish Designs", "Edible Excellence",
            "Food Foundations", "Taste Traditions", "Cuisine Creators"
        ]
        
        for i, name in enumerate(vendor_names):
            vendor = Vendor(name=name)
            vendors.append(vendor)
            db.session.add(vendor)
        
        db.session.commit()
        print(f"Created {len(vendors)} vendors")
        
        # Create kitchens (10 per location)
        kitchens = []
        for location in locations:
            for i in range(1, 11):  # 10 kitchens per location
                kitchen = Kitchen(
                    name=f"K{i} - {location.name}",
                    location_id=location.id,
                    vendor_id=vendors[random.randint(0, len(vendors)-1)].id  # Random vendor
                )
                kitchens.append(kitchen)
                db.session.add(kitchen)
        
        db.session.commit()
        print(f"Created {len(kitchens)} kitchens")
        
        # Create brands (20 per location)
        all_brands = []
        for location in locations:
            # Get kitchens for this location
            location_kitchens = [k for k in kitchens if k.location_id == location.id]
            
            # Create 20 brands for this location
            for i in range(20):
                # Pick a random vendor for this brand
                vendor = random.choice(vendors)
                
                # Generate a brand name
                brand_name = generate_brand_name()
                
                # Create the brand
                brand = Brand(
                    name=brand_name,
                    vendor_id=vendor.id
                )
                db.session.add(brand)
                
                # We need to commit to get the brand id for creating menu later
                db.session.flush()
                
                # Assign this brand to 1-3 random kitchens from this location
                num_kitchens = random.randint(1, 3)
                selected_kitchens = random.sample(location_kitchens, min(num_kitchens, len(location_kitchens)))
                
                for kitchen in selected_kitchens:
                    brand.kitchens.append(kitchen)
                
                all_brands.append(brand)
        
        db.session.commit()
        print(f"Created {len(all_brands)} brands with kitchen assignments")
        
        # Create menus, categories, and items
        for brand in all_brands:
            # Create a menu for this brand (if it doesn't already have one)
            menu = Menu.query.filter_by(brand_id=brand.id).first()
            if not menu:
                menu = Menu(brand_id=brand.id)
                db.session.add(menu)
                db.session.flush()
            
            # Decide how many and which categories this menu will have (3-6 categories)
            num_categories = random.randint(3, 6)
            selected_categories = random.sample(CATEGORIES, num_categories)
            
            # Create the categories
            for cat_name in selected_categories:
                category = Category(
                    name=cat_name,
                    menu_id=menu.id
                )
                db.session.add(category)
                db.session.flush()
                
                # Add items to this category (3-7 items per category)
                menu_items = MENU_ITEMS.get(cat_name, MENU_ITEMS["Main Courses"])
                
                # If there are enough items in our template, pick a random selection
                # Otherwise use all available items and possibly repeat some
                if len(menu_items) > 7:
                    selected_items = random.sample(menu_items, random.randint(3, 7))
                else:
                    selected_items = menu_items
                
                for item_data in selected_items:
                    # Slightly vary the price for more realism
                    price_variation = random.uniform(0.9, 1.1)
                    adjusted_price = round(item_data["price"] * price_variation, 2)
                    
                    item = Item(
                        name=item_data["name"],
                        description=item_data["description"],
                        price=adjusted_price,
                        category_id=category.id
                    )
                    db.session.add(item)
        
        db.session.commit()
        print("Created menus, categories and items for all brands")
        
        print("Database seeding completed successfully!")
        return True
        
    except Exception as e:
        db.session.rollback()
        print(f"Error seeding database: {str(e)}")
        return False

if __name__ == "__main__":
    app = create_app()
    with app.app_context():
        # Check if the database already has data
        has_data = db.session.query(Location.id).first() is not None
        
        if has_data and len(sys.argv) < 2:
            print("Database already contains data. To reseed, run with --force flag.")
            print("WARNING: This will delete all existing data.")
            sys.exit(1)
        
        # If --force flag is provided, wipe the database and reseed
        if len(sys.argv) > 1 and sys.argv[1] == "--force":
            print("Force flag detected. Dropping all tables...")
            db.drop_all()
            db.create_all()
        
        seed_database()
