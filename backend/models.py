from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

# Many-to-many association table between Brand and Kitchen
brand_kitchen_association = db.Table(
    "brand_kitchen",
    db.Column(
        "brand_id",
        db.Integer,
        db.ForeignKey("brand.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    db.Column(
        "kitchen_id",
        db.Integer,
        db.ForeignKey("kitchen.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)


class Location(db.Model):
    __tablename__ = "location"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    address = db.Column(db.String(200), nullable=True)

    # If location is deleted, cascade delete all kitchens in that location
    kitchens = db.relationship(
        "Kitchen", backref="location", lazy=True, cascade="all, delete-orphan"
    )


class Vendor(db.Model):
    __tablename__ = "vendor"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)

    # when vendor is deleted, just set kitchen.vendor_id to NULL
    kitchens = db.relationship(
        "Kitchen", backref="vendor", lazy=True, passive_deletes=False
    )
    # when vendor is deleted, cascade delete its brands
    brands = db.relationship(
        "Brand", backref="vendor", lazy=True, cascade="all, delete-orphan"
    )


class Kitchen(db.Model):
    __tablename__ = "kitchen"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    location_id = db.Column(
        db.Integer, db.ForeignKey("location.id", ondelete="CASCADE"), nullable=False
    )
    vendor_id = db.Column(
        db.Integer, db.ForeignKey("vendor.id", ondelete="SET NULL"), nullable=True
    )


class Brand(db.Model):
    __tablename__ = "brand"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    vendor_id = db.Column(
        db.Integer, db.ForeignKey("vendor.id", ondelete="CASCADE"), nullable=False
    )

    # brand <-> kitchens many-to-many (when brand is deleted, just remove associations)
    kitchens = db.relationship(
        "Kitchen",
        secondary=brand_kitchen_association,
        lazy="subquery",
        backref=db.backref("brands", lazy="subquery"),
        cascade="all, delete",  # Remove associations but don't delete kitchens
        single_parent=False,
    )

    # when brand is deleted, cascade delete its menu
    menu = db.relationship(
        "Menu", backref="brand", uselist=False, cascade="all, delete-orphan"
    )


class Menu(db.Model):
    __tablename__ = "menu"

    id = db.Column(db.Integer, primary_key=True)
    brand_id = db.Column(
        db.Integer, db.ForeignKey("brand.id", ondelete="CASCADE"), nullable=False
    )

    # when menu is deleted, cascade delete categories
    categories = db.relationship(
        "Category", backref="menu", lazy=True, cascade="all, delete-orphan"
    )


class Category(db.Model):
    __tablename__ = "category"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    menu_id = db.Column(
        db.Integer, db.ForeignKey("menu.id", ondelete="CASCADE"), nullable=False
    )

    # when category is deleted, cascade delete items
    items = db.relationship(
        "Item", backref="category", lazy=True, cascade="all, delete-orphan"
    )


class Item(db.Model):
    __tablename__ = "item"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    description = db.Column(db.Text, nullable=True)
    price = db.Column(db.Float, nullable=False, default=0.0)
    image_data = db.Column(db.Text, nullable=True)  # Storing Base64 or BLOB as text

    category_id = db.Column(
        db.Integer, db.ForeignKey("category.id", ondelete="CASCADE"), nullable=False
    )
