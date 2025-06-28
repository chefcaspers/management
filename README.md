# Chef Casper's Management System

## Overview

Chef Casper's Management System is a comprehensive restaurant management application
designed for multi-brand food service operations. It provides an intuitive interface
for managing menus, inventory, orders, and brand-specific configurations across multiple
restaurant brands under one management system.

## Entities and Relationships

The application is structured around these key entities:

- **Locations**: Physical locations where kitchens operate
- **Vendors**: Companies that own and operate brands and kitchens
- **Kitchens**: Physical cooking spaces within locations, operated by vendors
- **Brands**: Distinct restaurant brands owned by vendors and operating in specific kitchens
- **Menus**: Each brand has one menu that contains categories
- **Categories**: Organizational sections of a menu (e.g., Appetizers, Entrees)
- **Items**: Individual food/drink products with details like price, description, and images

### Entity Relationships

```
Location (1) ---> Kitchen (many) <---> Brand (many) <--- Vendor (1)
                                         |
                                         v
                                     Menu (1)
                                         |
                                         v
                                  Category (many)
                                         |
                                         v
                                     Item (many)
```

Key relationships:
- Each Location can have multiple Kitchens
- Each Vendor can own multiple Brands and Kitchens
- Brands and Kitchens have a many-to-many relationship (brands can operate in multiple kitchens)
- Each Brand has exactly one Menu
- Each Menu has multiple Categories
- Each Category contains multiple Items

## Technical Documentation

Can run with embedded sqlite or remote postgres instance

To run:

```
cd backend
poetry install # for dependencies
DB_HOST= DB_USER= DB_PASSWORD= DB_NAME= # optional environment for hooking up to remote postgres
python app.py
```

```
cd frontend
npm install
npm start
```

