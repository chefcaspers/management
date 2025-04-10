// src/pages/KitchensPage.js
import React, { useState } from 'react';
import { createKitchen, deleteKitchen, updateKitchen } from '../api';
import PageHeader from '../components/PageHeader';
import Table from '../components/Table';
import EditModal from '../components/EditModal';

function KitchensPage({ locations, vendors, kitchens, refetchAll }) {
  const [kitchenName, setKitchenName] = useState('');
  const [kitchenLocationId, setKitchenLocationId] = useState('');
  const [kitchenVendorId, setKitchenVendorId] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  
  // Edit modal state
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [currentKitchen, setCurrentKitchen] = useState(null);

  const handleCreateKitchen = async (e) => {
    e.preventDefault();
    if (!kitchenName || !kitchenLocationId) return;
    
    setIsSubmitting(true);
    try {
      await createKitchen({
        name: kitchenName,
        location_id: parseInt(kitchenLocationId),
        vendor_id: kitchenVendorId ? parseInt(kitchenVendorId) : null
      });
      setKitchenName('');
      setKitchenLocationId('');
      setKitchenVendorId('');
      refetchAll();
    } catch (error) {
      console.error("Failed to create kitchen:", error);
      alert("Could not create kitchen. Please try again.");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleDeleteKitchen = async (id) => {
    if (!confirm("Are you sure you want to delete this kitchen?")) {
      return;
    }
    
    try {
      await deleteKitchen(id);
      refetchAll();
    } catch (error) {
      console.error("Failed to delete kitchen:", error);
      alert("Could not delete kitchen. Please try again.");
    }
  };
  
  const handleEditKitchen = (kitchen) => {
    setCurrentKitchen(kitchen);
    setEditModalOpen(true);
  };
  
  const handleUpdateKitchen = async (formData) => {
    try {
      // Convert IDs to integers
      const updates = {
        ...formData,
        location_id: parseInt(formData.location_id),
        vendor_id: formData.vendor_id ? parseInt(formData.vendor_id) : null
      };
      await updateKitchen(currentKitchen.id, updates);
      refetchAll();
    } catch (error) {
      console.error("Failed to update kitchen:", error);
      throw error;
    }
  };

  // Create options arrays for dropdown selects
  const locationOptions = locations.map(loc => ({
    value: loc.id,
    label: loc.name
  }));
  
  const vendorOptions = vendors.map(vendor => ({
    value: vendor.id,
    label: vendor.name
  }));

  const columns = [
    { header: 'ID', key: 'id' },
    { header: 'Name', key: 'name' },
    { header: 'Location', key: 'location' },
    { header: 'Vendor', key: 'vendor' },
    { header: 'Associated Brands', key: 'brands' },
    { header: 'Actions', key: 'actions' },
  ];

  const renderCell = (row, key) => {
    if (key === 'location') {
      // Use the location_name property from the API response
      return row.location_name || '-';
    }
    if (key === 'vendor') {
      // Use the vendor_name property from the API response
      return row.vendor_name || '-';
    }
    if (key === 'brands') {
      if (!row.associated_brands || row.associated_brands.length === 0) {
        return <em>No brands</em>;
      }
      
      return (
        <ul style={{ margin: 0, paddingLeft: '1.2rem' }}>
          {row.associated_brands.map(brand => (
            <li key={brand.id}>{brand.name}</li>
          ))}
        </ul>
      );
    }
    if (key === 'actions') {
      return (
        <div className="grid" style={{ gap: '0.5rem' }}>
          <button 
            className="outline" 
            onClick={() => handleEditKitchen(row)}
            aria-label="Edit kitchen"
          >
            Edit
          </button>
          <button 
            className="secondary outline" 
            onClick={() => handleDeleteKitchen(row.id)}
            aria-label="Delete kitchen"
          >
            Delete
          </button>
        </div>
      );
    }
    return row[key] || '-';
  };

  // Fields configuration for the edit modal
  const kitchenFields = {
    name: {
      label: 'Name',
      type: 'text',
      required: true,
      placeholder: 'Kitchen Name'
    },
    location_id: {
      label: 'Location',
      type: 'select',
      required: true,
      options: locationOptions,
      placeholder: 'Select Location'
    },
    vendor_id: {
      label: 'Vendor',
      type: 'select',
      required: false,
      options: vendorOptions,
      placeholder: 'Select Vendor (optional)'
    }
  };

  return (
    <article>
      <PageHeader 
        title="Kitchens" 
        description="Manage your ghost kitchen spaces"
      />

      <form onSubmit={handleCreateKitchen}>
        <div className="grid">
          <label>
            Name
            <input
              type="text"
              placeholder="Kitchen Name"
              value={kitchenName}
              onChange={(e) => setKitchenName(e.target.value)}
              required
              disabled={isSubmitting}
            />
          </label>
          
          <label>
            Location
            <select
              value={kitchenLocationId}
              onChange={(e) => setKitchenLocationId(e.target.value)}
              required
              disabled={isSubmitting}
            >
              <option value="">Select Location</option>
              {locations.map(loc => (
                <option key={loc.id} value={loc.id}>{loc.name}</option>
              ))}
            </select>
          </label>
          
          <label>
            Vendor (Optional)
            <select
              value={kitchenVendorId}
              onChange={(e) => setKitchenVendorId(e.target.value)}
              disabled={isSubmitting}
            >
              <option value="">Select Vendor (optional)</option>
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
              {isSubmitting ? 'Adding...' : 'Add Kitchen'}
            </button>
          </div>
        </div>
      </form>

      <Table 
        columns={columns}
        data={kitchens}
        renderCell={renderCell}
      />
      
      <EditModal
        isOpen={editModalOpen}
        onClose={() => setEditModalOpen(false)}
        title="Edit Kitchen"
        data={currentKitchen}
        fields={kitchenFields}
        onSubmit={handleUpdateKitchen}
      />
    </article>
  );
}

export default KitchensPage;
