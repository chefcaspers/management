import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from models import db, Location, Kitchen, Vendor, Brand, Menu, Category, Item, brand_kitchen_association
from sqlalchemy.engine import URL

def create_app():
    app = Flask(__name__)
    CORS(app)

    # Use environment variables to decide between SQLite or Postgres
    db_host = os.getenv("DB_HOST")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_name = os.getenv("DB_NAME")
    if db_host and db_user and db_password and db_name:
        url_object = URL.create(
            "postgresql",
            username=db_user,
            password=db_password,
            host=db_host,
            database=db_name,
        )
        app.config['SQLALCHEMY_DATABASE_URI'] = url_object.render_as_string(hide_password=False)
    else:
        # Fall back to SQLite
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///ghost_kitchen.db'

    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    db.init_app(app)

    with app.app_context():
        db.create_all()  # Create tables if they don't exist

    return app

app = create_app()

# --------------
#  LOCATION CRUD
# --------------
@app.route('/api/locations', methods=['GET'])
def get_locations():
    locations = Location.query.all()
    results = []
    for loc in locations:
        results.append({
            'id': loc.id,
            'name': loc.name,
            'address': loc.address
        })
    return jsonify(results)

@app.route('/api/locations', methods=['POST'])
def create_location():
    data = request.json
    loc = Location(name=data['name'], address=data.get('address'))
    db.session.add(loc)
    db.session.commit()
    return jsonify({'message': 'Location created', 'id': loc.id})

@app.route('/api/locations/<int:location_id>', methods=['PUT'])
def update_location(location_id):
    loc = Location.query.get(location_id)
    if not loc:
        return jsonify({'error': 'Not found'}), 404

    data = request.json
    loc.name = data.get('name', loc.name)
    loc.address = data.get('address', loc.address)
    db.session.commit()
    return jsonify({'message': 'Location updated'})

@app.route('/api/locations/<int:location_id>', methods=['DELETE'])
def delete_location(location_id):
    loc = Location.query.get(location_id)
    if not loc:
        return jsonify({'error': 'Not found'}), 404
    db.session.delete(loc)
    db.session.commit()
    return jsonify({'message': 'Location deleted'})

# -----------
#  VENDOR CRUD
# -----------
@app.route('/api/vendors', methods=['GET'])
def get_vendors():
    vendors = Vendor.query.all()
    results = []
    for v in vendors:
        results.append({
            'id': v.id,
            'name': v.name
        })
    return jsonify(results)

@app.route('/api/vendors', methods=['POST'])
def create_vendor():
    data = request.json
    vendor = Vendor(name=data['name'])
    db.session.add(vendor)
    db.session.commit()
    return jsonify({'message': 'Vendor created', 'id': vendor.id})

@app.route('/api/vendors/<int:vendor_id>', methods=['PUT'])
def update_vendor(vendor_id):
    vendor = Vendor.query.get(vendor_id)
    if not vendor:
        return jsonify({'error': 'Not found'}), 404

    data = request.json
    vendor.name = data.get('name', vendor.name)
    db.session.commit()
    return jsonify({'message': 'Vendor updated'})

@app.route('/api/vendors/<int:vendor_id>', methods=['DELETE'])
def delete_vendor(vendor_id):
    vendor = Vendor.query.get(vendor_id)
    if not vendor:
        return jsonify({'error': 'Not found'}), 404
    db.session.delete(vendor)
    db.session.commit()
    return jsonify({'message': 'Vendor deleted'})

# ------------
#  KITCHEN CRUD
# ------------
@app.route('/api/kitchens', methods=['GET'])
def get_kitchens():
    kitchens = Kitchen.query.all()
    results = []
    for k in kitchens:
        # Get the location name
        location = Location.query.get(k.location_id)
        location_name = location.name if location else "Unknown"
        
        # Get the vendor name
        vendor = Vendor.query.get(k.vendor_id) if k.vendor_id else None
        vendor_name = vendor.name if vendor else None
        
        # Get associated brands
        associated_brands = []
        for brand in k.brands:
            associated_brands.append({
                'id': brand.id,
                'name': brand.name
            })
        
        results.append({
            'id': k.id,
            'name': k.name,
            'location_id': k.location_id,
            'location_name': location_name,
            'vendor_id': k.vendor_id,
            'vendor_name': vendor_name,
            'associated_brands': associated_brands
        })
    return jsonify(results)

@app.route('/api/kitchens', methods=['POST'])
def create_kitchen():
    data = request.json
    kitchen = Kitchen(
        name=data['name'],
        location_id=data['location_id'],
        vendor_id=data.get('vendor_id')
    )
    db.session.add(kitchen)
    db.session.commit()
    return jsonify({'message': 'Kitchen created', 'id': kitchen.id})

@app.route('/api/kitchens/<int:kitchen_id>', methods=['PUT'])
def update_kitchen(kitchen_id):
    kitchen = Kitchen.query.get(kitchen_id)
    if not kitchen:
        return jsonify({'error': 'Not found'}), 404

    data = request.json
    kitchen.name = data.get('name', kitchen.name)
    kitchen.location_id = data.get('location_id', kitchen.location_id)
    kitchen.vendor_id = data.get('vendor_id', kitchen.vendor_id)
    db.session.commit()
    return jsonify({'message': 'Kitchen updated'})

@app.route('/api/kitchens/<int:kitchen_id>', methods=['DELETE'])
def delete_kitchen(kitchen_id):
    kitchen = Kitchen.query.get(kitchen_id)
    if not kitchen:
        return jsonify({'error': 'Not found'}), 404
    db.session.delete(kitchen)
    db.session.commit()
    return jsonify({'message': 'Kitchen deleted'})

# -----------
#  BRAND CRUD
# -----------
@app.route('/api/brands', methods=['GET'])
def get_brands():
    brands = Brand.query.all()
    results = []
    for b in brands:
        # Get the vendor name
        vendor = Vendor.query.get(b.vendor_id)
        vendor_name = vendor.name if vendor else "Unknown"
        
        # Get associated kitchens
        associated_kitchens = []
        for kitchen in b.kitchens:
            location = Location.query.get(kitchen.location_id)
            associated_kitchens.append({
                'id': kitchen.id,
                'name': kitchen.name,
                'location_id': kitchen.location_id,
                'location_name': location.name if location else "Unknown"
            })
        
        results.append({
            'id': b.id,
            'name': b.name,
            'vendor_id': b.vendor_id,
            'vendor_name': vendor_name,
            'associated_kitchens': associated_kitchens
        })
    return jsonify(results)

@app.route('/api/brands', methods=['POST'])
def create_brand():
    data = request.json
    brand = Brand(name=data['name'], vendor_id=data['vendor_id'])
    db.session.add(brand)
    db.session.commit()
    # auto-create a Menu for this Brand
    menu = Menu(brand_id=brand.id)
    db.session.add(menu)
    db.session.commit()
    return jsonify({'message': 'Brand created', 'id': brand.id})

@app.route('/api/brands/<int:brand_id>', methods=['PUT'])
def update_brand(brand_id):
    brand = Brand.query.get(brand_id)
    if not brand:
        return jsonify({'error': 'Not found'}), 404

    data = request.json
    brand.name = data.get('name', brand.name)
    db.session.commit()
    return jsonify({'message': 'Brand updated'})

@app.route('/api/brands/<int:brand_id>', methods=['DELETE'])
def delete_brand(brand_id):
    brand = Brand.query.get(brand_id)
    if not brand:
        return jsonify({'error': 'Not found'}), 404
    # also delete menu if desired
    if brand.menu:
        db.session.delete(brand.menu)
    db.session.delete(brand)
    db.session.commit()
    return jsonify({'message': 'Brand deleted'})

# ---------------------------------
#  BRAND <--> KITCHEN RELATIONSHIP
# ---------------------------------
@app.route('/api/brands/<int:brand_id>/kitchens', methods=['POST'])
def associate_brand_with_kitchens(brand_id):
    """Accepts a list of kitchen_ids to associate with the brand."""
    brand = Brand.query.get(brand_id)
    if not brand:
        return jsonify({'error': 'Brand not found'}), 404

    data = request.json
    kitchen_ids = data.get('kitchen_ids', [])
    brand.kitchens = []
    for k_id in kitchen_ids:
        kitchen = Kitchen.query.get(k_id)
        if kitchen:
            brand.kitchens.append(kitchen)
    db.session.commit()
    return jsonify({'message': 'Brand-Kitchen association updated'})

# ----------
#  MENU CRUD
# ----------
@app.route('/api/menus', methods=['GET'])
def get_menus():
    menus = Menu.query.all()
    results = []
    for m in menus:
        results.append({
            'id': m.id,
            'brand_id': m.brand_id
        })
    return jsonify(results)

# Typically we auto-create menu with the brand, so you might not need a separate POST.
# But in case you do:

@app.route('/api/menus', methods=['POST'])
def create_menu():
    data = request.json
    menu = Menu(brand_id=data['brand_id'])
    db.session.add(menu)
    db.session.commit()
    return jsonify({'message': 'Menu created', 'id': menu.id})

@app.route('/api/menus/<int:menu_id>', methods=['PUT'])
def update_menu(menu_id):
    menu = Menu.query.get(menu_id)
    if not menu:
        return jsonify({'error': 'Not found'}), 404
    
    data = request.json
    # Currently only brand_id can be updated
    if 'brand_id' in data:
        menu.brand_id = data['brand_id']
    
    db.session.commit()
    return jsonify({'message': 'Menu updated'})

@app.route('/api/menus/<int:menu_id>', methods=['DELETE'])
def delete_menu(menu_id):
    menu = Menu.query.get(menu_id)
    if not menu:
        return jsonify({'error': 'Not found'}), 404
    db.session.delete(menu)
    db.session.commit()
    return jsonify({'message': 'Menu deleted'})

# -------------
#  CATEGORY CRUD
# -------------
@app.route('/api/categories', methods=['GET'])
def get_categories():
    categories = Category.query.all()
    results = []
    for c in categories:
        results.append({
            'id': c.id,
            'name': c.name,
            'menu_id': c.menu_id
        })
    return jsonify(results)

@app.route('/api/categories', methods=['POST'])
def create_category():
    data = request.json
    category = Category(name=data['name'], menu_id=data['menu_id'])
    db.session.add(category)
    db.session.commit()
    # Return the full category object instead of just message and ID
    return jsonify({
        'id': category.id,
        'name': category.name,
        'menu_id': category.menu_id
    })

@app.route('/api/categories/<int:category_id>', methods=['PUT'])
def update_category(category_id):
    category = Category.query.get(category_id)
    if not category:
        return jsonify({'error': 'Not found'}), 404
    data = request.json
    category.name = data.get('name', category.name)
    db.session.commit()
    # Return the full category object
    return jsonify({
        'id': category.id,
        'name': category.name,
        'menu_id': category.menu_id
    })

@app.route('/api/categories/<int:category_id>', methods=['DELETE'])
def delete_category(category_id):
    category = Category.query.get(category_id)
    if not category:
        return jsonify({'error': 'Not found'}), 404
    db.session.delete(category)
    db.session.commit()
    return jsonify({'message': 'Category deleted'})

# ---------
#  ITEM CRUD
# ---------
@app.route('/api/items', methods=['GET'])
def get_items():
    items = Item.query.all()
    results = []
    for i in items:
        results.append({
            'id': i.id,
            'name': i.name,
            'description': i.description,
            'price': i.price,
            'image_data': i.image_data,
            'category_id': i.category_id
        })
    return jsonify(results)

@app.route('/api/items', methods=['POST'])
def create_item():
    data = request.json
    item = Item(
        name=data['name'],
        description=data.get('description'),
        price=data.get('price', 0.0),
        image_data=data.get('image_data'),  # base64 string
        category_id=data['category_id']
    )
    db.session.add(item)
    db.session.commit()
    return jsonify({'message': 'Item created', 'id': item.id})

@app.route('/api/items/<int:item_id>', methods=['PUT'])
def update_item(item_id):
    item = Item.query.get(item_id)
    if not item:
        return jsonify({'error': 'Not found'}), 404
    data = request.json
    item.name = data.get('name', item.name)
    item.description = data.get('description', item.description)
    item.price = data.get('price', item.price)
    item.image_data = data.get('image_data', item.image_data)
    item.category_id = data.get('category_id', item.category_id)
    db.session.commit()
    return jsonify({'message': 'Item updated'})

@app.route('/api/items/<int:item_id>', methods=['DELETE'])
def delete_item(item_id):
    item = Item.query.get(item_id)
    if not item:
        return jsonify({'error': 'Not found'}), 404
    db.session.delete(item)
    db.session.commit()
    return jsonify({'message': 'Item deleted'})

if __name__ == '__main__':
    app.run(debug=True)
