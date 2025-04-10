// src/pages/BrandsPage.js
import React, { useState, useEffect } from 'react';
import {
  createBrand,
  deleteBrand,
  updateBrand,
  associateBrandWithKitchens
} from '../api';
import PageHeader from '../components/PageHeader';
import Table from '../components/Table';
import Modal from '../components/Modal';
import EditModal from '../components/EditModal';

function BrandsPage({ vendors, kitchens, brands, locations, refetchAll }) {
  const [brandName, setBrandName] = useState('');
  const [brandVendorId, setBrandVendorId] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Edit modal state
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [currentBrand, setCurrentBrand] = useState(null);

  // For brand ↔ kitchen association
  const [selectedBrandForKitchens, setSelectedBrandForKitchens] = useState(null);
  const [brandKitchenSelections, setBrandKitchenSelections] = useState([]);

  const handleCreateBrand = async (e) => {
    e.preventDefault();
    if (!brandName || !brandVendorId) return;
    
    setIsSubmitting(true);
    try {
      await createBrand({ name: brandName, vendor_id: parseInt(brandVendorId) });
      setBrandName('');
      setBrandVendorId('');
      refetchAll();
    } catch (error) {
      console.error("Failed to create brand:", error);
      alert("Could not create brand. Please try again.");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleDeleteBrand = async (id) => {
    if (!confirm("Are you sure you want to delete this brand? This will delete the menu and all associated items.")) {
      return;
    }
    
    try {
      await deleteBrand(id);
      refetchAll();
    } catch (error) {
      console.error("Failed to delete brand:", error);
      alert("Could not delete brand. Please try again.");
    }
  };
  
  const handleEditBrand = (brand) => {
    setCurrentBrand(brand);
    setEditModalOpen(true);
  };
  
  const handleUpdateBrand = async (formData) => {
    try {
      // Convert vendor_id to integer
      const updates = {
        ...formData,
        vendor_id: parseInt(formData.vendor_id)
      };
      await updateBrand(currentBrand.id, updates);
      refetchAll();
    } catch (error) {
      console.error("Failed to update brand:", error);
      throw error;
    }
  };

  // Get associated kitchens for a brand
  const getBrandKitchens = (brand) => {
    // Use the associated_kitchens property from the API response if available
    if (brand.associated_kitchens && Array.isArray(brand.associated_kitchens)) {
      return brand.associated_kitchens;
    }
    
    // Fallback to the old method if needed
    return kitchens.filter(k => 
      k.associated_brands && k.associated_brands.some(b => b.id === brand.id)
    );
  };

  // Open kitchen association modal with pre-selected kitchens
  const openBrandKitchenModal = async (brand) => {
    setSelectedBrandForKitchens(brand);
    
    // Get current kitchen associations for this brand
    const brandKitchens = getBrandKitchens(brand);
    
    // Pre-select current associations
    setBrandKitchenSelections(brandKitchens.map(k => k.id.toString()));
  };

  const handleAssociateBrandWithKitchens = async (e) => {
    e.preventDefault();
    if (!selectedBrandForKitchens) return;
    
    try {
      await associateBrandWithKitchens(
        selectedBrandForKitchens.id,
        brandKitchenSelections.map(Number)
      );
      setSelectedBrandForKitchens(null);
      setBrandKitchenSelections([]);
      refetchAll();
    } catch (error) {
      console.error("Failed to associate brand with kitchens:", error);
      alert("Could not update kitchen associations. Please try again.");
    }
  };
  
  // Get location name for a kitchen
  const getLocationForKitchen = (kitchenId) => {
    const kitchen = kitchens.find(k => k.id === kitchenId);
    if (!kitchen) return 'Unknown';
    
    const location = locations.find(l => l.id === kitchen.location_id);
    return location ? location.name : 'Unknown';
  };
  
  // Create options array for vendor dropdown
  const vendorOptions = vendors.map(vendor => ({
    value: vendor.id,
    label: vendor.name
  }));

  // Updated columns to include associated kitchens
  const columns = [
    { header: 'ID', key: 'id' },
    { header: 'Name', key: 'name' },
    { header: 'Vendor', key: 'vendor' },
    { header: 'Kitchens', key: 'kitchens' },
    { header: 'Actions', key: 'actions' },
  ];

  const renderCell = (row, key) => {
    if (key === 'vendor') {
      return row.vendor_name || '-';
    }
    
    if (key === 'kitchens') {
      const brandKitchens = getBrandKitchens(row);
      
      if (brandKitchens.length === 0) {
        return <em>No kitchens</em>;
      }
      
      return (
        <ul style={{ margin: 0, paddingLeft: '1.2rem' }}>
          {brandKitchens.map(kitchen => (
            <li key={kitchen.id}>
              {kitchen.name} {kitchen.location_name ? `(${kitchen.location_name})` : ''}
            </li>
          ))}
        </ul>
      );
    }
    
    if (key === 'actions') {
      return (
        <div className="grid" style={{ gap: '0.5rem' }}>
          <button 
            className="outline" 
            onClick={() => handleEditBrand(row)}
            aria-label="Edit brand"
          >
            Edit
          </button>
          <button 
            className="outline" 
            onClick={() => openBrandKitchenModal(row)}
            aria-label="Associate kitchens"
          >
            Kitchens
          </button>
          <button 
            className="secondary outline" 
            onClick={() => handleDeleteBrand(row.id)}
            aria-label="Delete brand"
          >
            Delete
          </button>
        </div>
      );
    }
    
    return row[key] || '-';
  };

  // Fields configuration for the edit modal
  const brandFields = {
    name: {
      label: 'Name',
      type: 'text',
      required: true,
      placeholder: 'Brand Name'
    },
    vendor_id: {
      label: 'Vendor',
      type: 'select',
      required: true,
      options: vendorOptions,
      placeholder: 'Select Vendor'
    }
  };

  return (
    <article>
      <PageHeader 
        title="Brands" 
        description="Manage food service brands"
      />

      <form onSubmit={handleCreateBrand}>
        <div className="grid">
          <label>
            Name
            <input
              type="text"
              placeholder="Brand Name"
              value={brandName}
              onChange={(e) => setBrandName(e.target.value)}
              required
              disabled={isSubmitting}
            />
          </label>
          
          <label>
            Vendor
            <select
              value={brandVendorId}
              onChange={(e) => setBrandVendorId(e.target.value)}
              required
              disabled={isSubmitting}
            >
              <option value="">Select Vendor</option>
              {vendors.map(v => (
                <option key={v.id} value={v.id}>{v.name}</option>
              ))}
            </select>
          </label>
          
          <div>
            <label>&nbsp;</label>
            <button 
              type="submit" 
              aria-busy={isSubmitting}
              disabled={isSubmitting}
            >
              {isSubmitting ? 'Adding...' : 'Add Brand'}
            </button>
          </div>
        </div>
      </form>

      <Table 
        columns={columns}
        data={brands}
        renderCell={renderCell}
      />
      
      <EditModal
        isOpen={editModalOpen}
        onClose={() => setEditModalOpen(false)}
        title="Edit Brand"
        data={currentBrand}
        fields={brandFields}
        onSubmit={handleUpdateBrand}
      />

      {/* BRAND ↔ KITCHEN MODAL */}
      {selectedBrandForKitchens && (
        <Modal 
          isOpen={true} 
          onClose={() => setSelectedBrandForKitchens(null)}
          title={`Associate Kitchens with ${selectedBrandForKitchens.name}`}
        >
          <form onSubmit={handleAssociateBrandWithKitchens}>
            <p>Select kitchens for this brand:</p>
            <fieldset style={{ maxHeight: '250px', overflowY: 'auto' }}>
              <legend>Available Kitchens</legend>
              {kitchens
                .filter(k => k.vendor_id === selectedBrandForKitchens.vendor_id)
                .map(k => {
                  const locationName = k.location_name || 'Unknown location';
                  
                  return (
                    <label key={k.id} style={{ display: 'block', margin: '0.5rem 0' }}>
                      <input
                        type="checkbox"
                        value={k.id}
                        checked={brandKitchenSelections.includes(k.id.toString())}
                        onChange={(e) => {
                          const { value, checked } = e.target;
                          if (checked) {
                            setBrandKitchenSelections(prev => [...prev, value]);
                          } else {
                            setBrandKitchenSelections(prev => 
                              prev.filter(id => id !== value)
                            );
                          }
                        }}
                      />
                      {' '}{k.name} <span style={{ color: '#666', fontSize: '0.9em' }}>({locationName})</span>
                    </label>
                  );
                })}
              {kitchens.filter(k => k.vendor_id === selectedBrandForKitchens.vendor_id).length === 0 && (
                <p><em>No kitchens available for this vendor</em></p>
              )}
            </fieldset>
            
            <div className="grid" style={{ marginTop: '1.5rem' }}>
              <button 
                type="button" 
                className="secondary"
                onClick={() => setSelectedBrandForKitchens(null)}
              >
                Cancel
              </button>
              <button type="submit">
                Save Associations
              </button>
            </div>
          </form>
        </Modal>
      )}
    </article>
  );
}

export default BrandsPage;
