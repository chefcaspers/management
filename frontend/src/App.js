// src/App.js
import React, { useEffect, useState } from 'react';
import NavBar from './NavBar';

// Import all pages
import LocationsPage from './pages/LocationsPage';
import VendorsPage from './pages/VendorsPage';
import KitchensPage from './pages/KitchensPage';
import BrandsPage from './pages/BrandsPage';
import MenusPage from './pages/MenusPage';

// API calls
import {
  getLocations,
  getVendors,
  getKitchens,
  getBrands,
  getMenus,
  getCategories,
  getItems
} from './api';

// Add a loading spinner component
const LoadingSpinner = () => (
  <div className="container">
    <div className="grid">
      <div style={{ textAlign: 'center', padding: '2rem' }}>
        <div className="loading" style={{ display: 'inline-block' }}></div>
      </div>
    </div>
  </div>
);

function App() {
  const [activeTab, setActiveTab] = useState('Locations');

  // Data states
  const [locations, setLocations] = useState([]);
  const [vendors, setVendors] = useState([]);
  const [kitchens, setKitchens] = useState([]);
  const [brands, setBrands] = useState([]);
  const [menus, setMenus] = useState([]);
  const [categories, setCategories] = useState([]);
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    refetchAllData();
  }, []);

  const refetchAllData = async () => {
    setLoading(true);
    try {
      const [
        locRes,
        venRes,
        kitRes,
        brRes,
        menuRes,
        catRes,
        itemRes
      ] = await Promise.all([
        getLocations(),
        getVendors(),
        getKitchens(),
        getBrands(),
        getMenus(),
        getCategories(),
        getItems()
      ]);
      
      // Ensure we have proper data with relationships
      setLocations(locRes.data);
      setVendors(venRes.data);
      setKitchens(kitRes.data);
      setBrands(brRes.data);
      setMenus(menuRes.data);
      setCategories(catRes.data);
      setItems(itemRes.data);
      
    } catch (err) {
      console.error('Error fetching data:', err);
    } finally {
      setLoading(false);
    }
  };

  // Render the current tab's page
  let content;
  
  if (loading) {
    content = <LoadingSpinner />;
  } else {
    switch (activeTab) {
      case 'Locations':
        content = (
          <LocationsPage
            locations={locations}
            refetchAll={refetchAllData}
          />
        );
        break;
      case 'Vendors':
        content = (
          <VendorsPage
            vendors={vendors}
            refetchAll={refetchAllData}
          />
        );
        break;
      case 'Kitchens':
        content = (
          <KitchensPage
            locations={locations}
            vendors={vendors}
            kitchens={kitchens}
            refetchAll={refetchAllData}
          />
        );
        break;
      case 'Brands':
        content = (
          <BrandsPage
            vendors={vendors}
            kitchens={kitchens}
            brands={brands}
            locations={locations}
            refetchAll={refetchAllData}
          />
        );
        break;
      case 'Menus':
        content = (
          <MenusPage
            brands={brands}
            menus={menus}
            categories={categories}
            items={items}
            refetchAll={refetchAllData}
          />
        );
        break;
      default:
        content = <div className="container">Select a tab</div>;
    }
  }

  // Layout: left nav, right content using Pico grid system
  return (
    <main className="container-fluid" style={{ padding: 0 }}>
      <header className="container-fluid" style={{ 
        background: 'var(--card-background-color)', 
        padding: '2rem 0', 
        marginBottom: '2rem',
        boxShadow: '0 1px 3px rgba(0,0,0,0.1)'
      }}>
        <div className="container">
          <hgroup style={{ textAlign: 'center', margin: 0 }}>
            <h1 style={{ marginBottom: '0.5rem' }}>Ghost Kitchen Admin</h1>
            <h2 style={{ opacity: 0.8, fontWeight: 'normal', marginTop: 0 }}>Management Portal</h2>
          </hgroup>
        </div>
      </header>
      
      <div className="container">
        <div className="grid" style={{ 
          gap: '2rem',
          alignItems: 'start',
          gridTemplateColumns: '250px 1fr' // Make nav take fixed width, content takes remaining space
        }}>
          <aside style={{ 
            padding: '1.5rem', 
            background: 'var(--card-background-color)', 
            borderRadius: 'var(--border-radius)',
            boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
            width: '100%' // Ensure it stays within its grid cell
          }}>
            <NavBar activeTab={activeTab} setActiveTab={setActiveTab} />
          </aside>
          <section style={{ 
            flex: 1,
            minWidth: 0, /* Prevents flex items from overflowing */
            background: 'var(--card-background-color)', 
            padding: '1.5rem',
            borderRadius: 'var(--border-radius)',
            boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
            width: '100%' // Ensure it fills its grid cell
          }}>
            {content}
          </section>
        </div>
      </div>
      
      <footer className="container" style={{ 
        marginTop: '3rem', 
        textAlign: 'center',
        padding: '2rem 0'
      }}>
        <small>© 2023 Chef Casper's Ghost Kitchen</small>
      </footer>
    </main>
  );
}

export default App;
