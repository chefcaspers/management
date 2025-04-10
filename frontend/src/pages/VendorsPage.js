// src/pages/VendorsPage.js
import React, { useState } from 'react';
import { createVendor, deleteVendor, updateVendor } from '../api';
import PageHeader from '../components/PageHeader';
import Table from '../components/Table';
import EditModal from '../components/EditModal';

function VendorsPage({ vendors, refetchAll }) {
  const [vendorName, setVendorName] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  
  // Edit modal state
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [currentVendor, setCurrentVendor] = useState(null);

  const handleCreateVendor = async (e) => {
    e.preventDefault();
    if (!vendorName) return;
    
    setIsSubmitting(true);
    try {
      await createVendor({ name: vendorName });
      setVendorName('');
      refetchAll();
    } catch (error) {
      console.error("Failed to create vendor:", error);
      alert("Could not create vendor. Please try again.");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleDeleteVendor = async (id) => {
    if (!confirm("Are you sure you want to delete this vendor? This will delete all brands associated with it.")) {
      return;
    }
    
    try {
      await deleteVendor(id);
      refetchAll();
    } catch (error) {
      console.error("Failed to delete vendor:", error);
      alert("Could not delete vendor. Please try again.");
    }
  };
  
  const handleEditVendor = (vendor) => {
    setCurrentVendor(vendor);
    setEditModalOpen(true);
  };
  
  const handleUpdateVendor = async (formData) => {
    try {
      await updateVendor(currentVendor.id, formData);
      refetchAll();
    } catch (error) {
      console.error("Failed to update vendor:", error);
      throw error;
    }
  };

  const columns = [
    { header: 'ID', key: 'id' },
    { header: 'Name', key: 'name' },
    { header: 'Actions', key: 'actions' },
  ];

  const renderCell = (row, key) => {
    if (key === 'actions') {
      return (
        <div className="grid" style={{ gap: '0.5rem' }}>
          <button 
            className="outline" 
            onClick={() => handleEditVendor(row)}
            aria-label="Edit vendor"
          >
            Edit
          </button>
          <button 
            className="secondary outline" 
            onClick={() => handleDeleteVendor(row.id)}
            aria-label="Delete vendor"
          >
            Delete
          </button>
        </div>
      );
    }
    return row[key];
  };

  // Fields configuration for the edit modal
  const vendorFields = {
    name: {
      label: 'Name',
      type: 'text',
      required: true,
      placeholder: 'Vendor Name'
    }
  };

  return (
    <article>
      <PageHeader 
        title="Vendors" 
        description="Manage food service vendors"
      />

      <form onSubmit={handleCreateVendor}>
        <div className="grid">
          <label>
            Name
            <input
              type="text"
              placeholder="Vendor Name"
              value={vendorName}
              onChange={(e) => setVendorName(e.target.value)}
              required
              disabled={isSubmitting}
            />
          </label>
          
          <div>
            <label>&nbsp;</label>
            <button 
              type="submit" 
              aria-busy={isSubmitting}
              disabled={isSubmitting}
            >
              {isSubmitting ? 'Adding...' : 'Add Vendor'}
            </button>
          </div>
        </div>
      </form>

      <Table 
        columns={columns}
        data={vendors}
        renderCell={renderCell}
      />
      
      <EditModal
        isOpen={editModalOpen}
        onClose={() => setEditModalOpen(false)}
        title="Edit Vendor"
        data={currentVendor}
        fields={vendorFields}
        onSubmit={handleUpdateVendor}
      />
    </article>
  );
}

export default VendorsPage;
