// src/NavBar.js
import React from 'react';

function NavBar({ activeTab, setActiveTab }) {
  const navItems = ['Locations', 'Vendors', 'Kitchens', 'Brands', 'Menus'];
  
  return (
    <nav>
      <ul>
        {navItems.map(item => (
          <li key={item}>
            <a 
              href="#" 
              onClick={(e) => {
                e.preventDefault();
                setActiveTab(item);
              }}
              className={activeTab === item ? 'contrast' : 'secondary'}
              role={activeTab === item ? 'button' : undefined}
              aria-current={activeTab === item ? 'page' : undefined}
            >
              {item}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  );
}

export default NavBar;
