// src/pages/LocationsPage.js
import React, { useState } from 'react';
import { createLocation, deleteLocation, updateLocation } from '../api';
import PageHeader from '../components/PageHeader';
import Table from '../components/Table';
import EditModal from '../components/EditModal';

function LocationsPage({ locations, refetchAll }) {
  const [locationName, setLocationName] = useState('');
  const [locationAddress, setLocationAddress] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  
  // Edit modal state
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [currentLocation, setCurrentLocation] = useState(null);

  const handleCreateLocation = async (e) => {
    e.preventDefault();
    if (!locationName) return;
    
    setIsSubmitting(true);
    try {
      await createLocation({ name: locationName, address: locationAddress });
      setLocationName('');
      setLocationAddress('');
      refetchAll();
    } catch (error) {
      console.error("Failed to create location:", error);
      alert("Could not create location. Please try again.");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleDeleteLocation = async (id) => {
    if (!confirm("Are you sure you want to delete this location? This will also delete all kitchens at this location.")) {
      return;
    }
    
    try {
      await deleteLocation(id);
      refetchAll();
    } catch (error) {
      console.error("Failed to delete location:", error);
      alert("Could not delete location. Please try again.");
    }
  };
  
  const handleEditLocation = (location) => {
    setCurrentLocation(location);
    setEditModalOpen(true);
  };
  
  const handleUpdateLocation = async (formData) => {
    try {
      await updateLocation(currentLocation.id, formData);
      refetchAll();
    } catch (error) {
      console.error("Failed to update location:", error);
      throw error;
    }
  };

  const columns = [
    { header: 'ID', key: 'id' },
    { header: 'Name', key: 'name' },
    { header: 'Address', key: 'address' },
    { header: 'Actions', key: 'actions' },
  ];

  const renderCell = (row, key) => {
    if (key === 'actions') {
      return (
        <div className="grid" style={{ gap: '0.5rem' }}>
          <button 
            className="outline" 
            onClick={() => handleEditLocation(row)}
            aria-label="Edit location"
          >
            Edit
          </button>
          <button 
            className="secondary outline" 
            onClick={() => handleDeleteLocation(row.id)}
            aria-label="Delete location"
          >
            Delete
          </button>
        </div>
      );
    }
    return row[key] || '-';
  };

  // Fields configuration for the edit modal
  const locationFields = {
    name: {
      label: 'Name',
      type: 'text',
      required: true,
      placeholder: 'Location Name'
    },
    address: {
      label: 'Address',
      type: 'text',
      placeholder: 'Location Address'
    }
  };

  return (
    <article>
      <PageHeader 
        title="Locations" 
        description="Manage your ghost kitchen locations"
      />

      <form onSubmit={handleCreateLocation}>
        <div className="grid">
          <label>
            Name
            <input
              type="text"
              placeholder="Location Name"
              value={locationName}
              onChange={(e) => setLocationName(e.target.value)}
              required
              disabled={isSubmitting}
            />
          </label>
          
          <label>
            Address
            <input
              type="text"
              placeholder="Address"
              value={locationAddress}
              onChange={(e) => setLocationAddress(e.target.value)}
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
              {isSubmitting ? 'Adding...' : 'Add Location'}
            </button>
          </div>
        </div>
      </form>

      <Table 
        columns={columns}
        data={locations}
        renderCell={renderCell}
      />
      
      <EditModal
        isOpen={editModalOpen}
        onClose={() => setEditModalOpen(false)}
        title="Edit Location"
        data={currentLocation}
        fields={locationFields}
        onSubmit={handleUpdateLocation}
      />
    </article>
  );
}

export default LocationsPage;
