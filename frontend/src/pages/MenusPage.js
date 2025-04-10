// src/pages/MenusPage.js
import React, { useState, useEffect } from 'react';
import {
  createCategory,
  deleteCategory,
  updateCategory,
  createItem,
  updateItem,
  deleteItem
} from '../api';
import PageHeader from '../components/PageHeader';
import Modal from '../components/Modal';
import EditModal from '../components/EditModal';

function MenusPage({ brands, menus, categories, items, refetchAll }) {
  const [selectedBrandId, setSelectedBrandId] = useState('');
  const [categoryName, setCategoryName] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [localCategories, setLocalCategories] = useState([]);
  const [localItems, setLocalItems] = useState([]);

  // Edit states
  const [editCategoryModal, setEditCategoryModal] = useState(false);
  const [currentCategory, setCurrentCategory] = useState(null);
  const [editItemModal, setEditItemModal] = useState(false);
  const [currentItem, setCurrentItem] = useState(null);
  
  // Create item modal state
  const [createItemModal, setCreateItemModal] = useState(false);
  const [selectedCategoryForItem, setSelectedCategoryForItem] = useState(null);

  // Restore brand selection from localStorage
  useEffect(() => {
    const savedBrandId = localStorage.getItem('selectedBrandId');
    if (savedBrandId && brands.some(b => b.id === parseInt(savedBrandId))) {
      setSelectedBrandId(savedBrandId);
    }
  }, [brands]);

  // Update local data when API data changes
  useEffect(() => {
    setLocalCategories(categories);
    setLocalItems(items);
  }, [categories, items]);

  // Save selected brand to localStorage
  useEffect(() => {
    if (selectedBrandId) {
      localStorage.setItem('selectedBrandId', selectedBrandId);
    }
  }, [selectedBrandId]);

  const getMenuForBrand = (brandId) => {
    return menus.find(m => m.brand_id === parseInt(brandId));
  };

  const handleCreateCategory = async (e) => {
    e.preventDefault();
    if (!selectedBrandId || !categoryName) return;
    const menuObj = getMenuForBrand(selectedBrandId);
    if (!menuObj) return;
  
    setIsSubmitting(true);
    try {
      await createCategory({ name: categoryName, menu_id: menuObj.id });
      setCategoryName('');
  
      // Instead of local state manip, just do a full re-fetch
      await refetchAll();
    } catch (error) {
      console.error("Failed to create category:", error);
      alert("Could not create category. Please try again.");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleDeleteCategory = async (id) => {
    if (!confirm("Are you sure you want to delete this category? This will delete all items in this category.")) {
      return;
    }
    
    try {
      await deleteCategory(id);
      
      // Update local state directly
      setLocalCategories(prevCategories => prevCategories.filter(c => c.id !== id));
      setLocalItems(prevItems => prevItems.filter(item => item.category_id !== id));
    } catch (error) {
      console.error("Failed to delete category:", error);
      alert("Could not delete category. Please try again.");
    }
  };
  
  const handleEditCategory = (category) => {
    setCurrentCategory(category);
    setEditCategoryModal(true);
  };
  
  const handleUpdateCategory = async (formData) => {
    try {
      // API call returns the full updated category object
      await updateCategory(currentCategory.id, formData);
      
      // Instead of local state manip, just do a full re-fetch
      await refetchAll();
      
      return formData;
    } catch (error) {
      console.error("Failed to update category:", error);
      alert("Could not update category. Please try again.");
      throw error;
    }
  };

  const openCreateItemModal = (category) => {
    setSelectedCategoryForItem(category);
    setCreateItemModal(true);
  };

  const handleCreateItem = async (formData) => {
    if (!selectedCategoryForItem) return;
    
    setIsSubmitting(true);
    try {
      const newItem = await createItem({
        ...formData,
        category_id: selectedCategoryForItem.id
      });
      
      // Update local state directly - make sure we create a new array reference
      setLocalItems(prevItems => [...prevItems, newItem]);
      setCreateItemModal(false);
      
      // Force refetch of items for the updated category
      refetchAll();
      
      return newItem;
    } catch (error) {
      console.error("Failed to create item:", error);
      alert("Could not create item. Please try again.");
      throw error;
    } finally {
      setIsSubmitting(false);
    }
  };
  
  const handleEditItem = (item) => {
    setCurrentItem(item);
    setEditItemModal(true);
  };
  
  const handleUpdateItem = async (formData) => {
    try {
      // Handle image file if it's a new one
      if (formData.newImageFile) {
        const base64Str = await fileToBase64(formData.newImageFile);
        formData.image_data = base64Str;
        delete formData.newImageFile;
      }
      
      // Convert price to float
      formData.price = parseFloat(formData.price || 0);
      
      const updatedItem = await updateItem(currentItem.id, formData);
      
      // Create new array with updated item to ensure re-render
      setLocalItems(prevItems => 
        prevItems.map(item => item.id === updatedItem.id ? updatedItem : item)
      );
      
      // Force refetch of items after update
      refetchAll();
      
      return updatedItem;
    } catch (error) {
      console.error("Failed to update item:", error);
      throw error;
    }
  };

  const handleDeleteItem = async (id) => {
    if (!confirm("Are you sure you want to delete this menu item?")) {
      return;
    }
    
    try {
      await deleteItem(id);
      
      // Update local state directly
      setLocalItems(prevItems => prevItems.filter(item => item.id !== id));
      
      // Force refetch of items after deletion
      refetchAll();
    } catch (error) {
      console.error("Failed to delete item:", error);
      alert("Could not delete item. Please try again.");
    }
  };

  // Helper to convert file -> base64
  const fileToBase64 = (file) => {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = (error) => reject(error);
      reader.readAsDataURL(file);
    });
  };

  // Filter categories for the selected brand's menu
  let menuCategories = [];
  if (selectedBrandId) {
    const menuObj = getMenuForBrand(selectedBrandId);
    if (menuObj) {
      menuCategories = localCategories.filter(c => c.menu_id === menuObj.id);
    }
  }
  
  // Fields for category edit modal
  const categoryFields = {
    name: {
      label: 'Name',
      type: 'text',
      required: true,
      placeholder: 'Category Name'
    }
  };
  
  // Fields for item edit modal
  const itemFields = {
    name: {
      label: 'Name',
      type: 'text',
      required: true,
      placeholder: 'Item Name'
    },
    description: {
      label: 'Description',
      type: 'textarea',
      required: false,
      placeholder: 'Item Description'
    },
    price: {
      label: 'Price',
      type: 'number',
      required: true,
      placeholder: '0.00',
      step: '0.01'
    },
    // We'll handle the image separately in the component
  };

  return (
    <article>
      <PageHeader 
        title="Menu Management" 
        description="Create and manage menu items for each brand"
      />
      
      <div style={{ marginBottom: '2rem' }}>
        <label htmlFor="brand-select">Select a Brand:</label>
        <select
          id="brand-select"
          value={selectedBrandId}
          onChange={(e) => setSelectedBrandId(e.target.value)}
          style={{ marginBottom: '1rem' }}
        >
          <option value="">-- Select Brand --</option>
          {brands.map(b => (
            <option key={b.id} value={b.id}>
              {b.name}
            </option>
          ))}
        </select>
      </div>

      {selectedBrandId && (
        <>
          <section style={{ marginBottom: '2rem' }}>
            <h3>Menu Categories</h3>
            <form onSubmit={handleCreateCategory} style={{ marginBottom: '1rem' }}>
              <div className="grid">
                <label style={{ marginBottom: 0 }}>
                  <span className="required-field">Category Name</span>
                  <input
                    type="text"
                    placeholder="Enter category name"
                    value={categoryName}
                    onChange={(e) => setCategoryName(e.target.value)}
                    required
                    disabled={isSubmitting}
                  />
                </label>
                <div style={{ display: 'flex', alignItems: 'flex-end' }}>
                  <button 
                    type="submit" 
                    disabled={isSubmitting}
                    aria-busy={isSubmitting}
                  >
                    {isSubmitting ? 'Adding...' : 'Add Category'}
                  </button>
                </div>
              </div>
            </form>

            {menuCategories.length === 0 ? (
              <p><em>No categories yet. Add your first category above.</em></p>
            ) : (
              <div className="categories-container">
                {menuCategories.map(cat => (
                  <div 
                    key={cat.id}
                    className="category-card"
                    style={{ 
                      border: '1px solid var(--form-element-border-color)', 
                      borderRadius: 'var(--border-radius)',
                      padding: '1rem', 
                      marginBottom: '1rem',
                      backgroundColor: 'var(--card-background-color, #fff)'
                    }}
                  >
                    <div style={{ 
                      display: 'flex', 
                      justifyContent: 'space-between',
                      alignItems: 'center',
                      marginBottom: '1rem',
                      borderBottom: '1px solid var(--form-element-border-color)',
                      paddingBottom: '0.5rem'
                    }}>
                      <h4 style={{ margin: 0 }}>{cat.name}</h4>
                      <div>
                        <button
                          className="outline primary"
                          onClick={() => openCreateItemModal(cat)}
                          style={{ marginRight: '0.5rem' }}
                        >
                          Add Item
                        </button>
                        <button
                          className="outline"
                          onClick={() => handleEditCategory(cat)}
                          style={{ marginRight: '0.5rem' }}
                        >
                          Edit
                        </button>
                        <button
                          className="secondary outline"
                          onClick={() => handleDeleteCategory(cat.id)}
                        >
                          Delete
                        </button>
                      </div>
                    </div>

                    {/* List Items in this category */}
                    <ItemList
                      cat={cat}
                      items={localItems}
                      handleDeleteItem={handleDeleteItem}
                      handleEditItem={handleEditItem}
                    />
                  </div>
                ))}
              </div>
            )}
          </section>
        </>
      )}
      
      <EditModal
        isOpen={editCategoryModal}
        onClose={() => setEditCategoryModal(false)}
        title="Edit Category"
        data={currentCategory}
        fields={categoryFields}
        onSubmit={handleUpdateCategory}
      />
      
      <EditItemModal 
        isOpen={editItemModal}
        onClose={() => setEditItemModal(false)}
        item={currentItem}
        onSubmit={handleUpdateItem}
      />

      <CreateItemModal 
        isOpen={createItemModal}
        onClose={() => setCreateItemModal(false)}
        onSubmit={handleCreateItem}
        categoryName={selectedCategoryForItem?.name}
        isSubmitting={isSubmitting}
      />
      
      <style jsx="true">{`
        .required-field::after {
          content: ' *';
          color: var(--form-element-invalid-color, #d81b60);
        }
        .categories-container {
          display: flex;
          flex-direction: column;
          gap: 1rem;
        }
        .category-card {
          width: 100%;
        }
      `}</style>
    </article>
  );
}

// Show items for a category
function ItemList({ cat, items, handleDeleteItem, handleEditItem }) {
  // Filter items for this category - do this filtering inside the component
  const catItems = React.useMemo(() => {
    return items.filter(i => i.category_id === cat.id);
  }, [items, cat.id]);
  
  if (catItems.length === 0) {
    return <p style={{ fontSize: '0.9em', color: '#666', marginBottom: '1rem' }}>No items yet.</p>;
  }
  
  return (
    <figure style={{ marginBottom: '1rem' }}>
      <table role="grid">
        <thead>
          <tr>
            <th>Name</th>
            <th>Price</th>
            <th>Description</th>
            <th>Image</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {catItems.map(item => (
            <tr key={item.id}>
              <td>{item.name}</td>
              <td>${item.price.toFixed(2)}</td>
              <td>{item.description || '-'}</td>
              <td>
                {item.image_data ? (
                  <img
                    src={item.image_data}
                    alt={item.name}
                    style={{ width: '50px', height: 'auto' }}
                  />
                ) : (
                  <span style={{ color: '#999' }}>No image</span>
                )}
              </td>
              <td>
                <div className="grid" style={{ gap: '0.25rem' }}>
                  <button
                    className="outline small"
                    onClick={() => handleEditItem(item)}
                  >
                    Edit
                  </button>
                  <button
                    className="secondary outline small"
                    onClick={() => handleDeleteItem(item.id)}
                  >
                    Delete
                  </button>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </figure>
  );
}

// Custom modal for editing items with image handling
function EditItemModal({ isOpen, onClose, item, onSubmit }) {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [price, setPrice] = useState('');
  const [imageFile, setImageFile] = useState(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  
  // Initialize form data when item changes
  React.useEffect(() => {
    if (item) {
      setName(item.name || '');
      setDescription(item.description || '');
      setPrice(item.price?.toString() || '');
      setImageFile(null);
    }
  }, [item]);
  
  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (!name) return;
    
    setIsSubmitting(true);
    try {
      const formData = {
        name,
        description,
        price,
      };
      
      if (imageFile) {
        formData.newImageFile = imageFile;
      }
      
      await onSubmit(formData);
      onClose();
    } catch (error) {
      console.error("Error updating item:", error);
      alert("An error occurred. Please try again.");
    } finally {
      setIsSubmitting(false);
    }
  };
  
  if (!isOpen || !item) return null;
  
  return (
    <Modal isOpen={isOpen} onClose={onClose} title="Edit Menu Item">
      <form onSubmit={handleSubmit}>
        <div className="grid">
          <label>
            <span className="required-field">Name</span>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              disabled={isSubmitting}
            />
          </label>
          
          <label>
            <span className="required-field">Price</span>
            <input
              type="number"
              step="0.01"
              value={price}
              onChange={(e) => setPrice(e.target.value)}
              required
              disabled={isSubmitting}
            />
          </label>
        </div>
        
        <label>
          Description <small>(optional)</small>
          <textarea
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            rows={3}
            disabled={isSubmitting}
          />
        </label>
        
        <div style={{ marginBottom: '1.5rem', marginTop: '1rem' }}>
          <label>Image <small>(optional)</small></label>
          
          {item.image_data && (
            <div style={{ margin: '0.5rem 0' }}>
              <img
                src={item.image_data}
                alt={item.name}
                style={{ maxWidth: '100px', height: 'auto' }}
              />
              <p><small>Current image shown above.</small></p>
            </div>
          )}
          
          <input
            type="file"
            accept="image/*"
            onChange={(e) => setImageFile(e.target.files[0])}
            disabled={isSubmitting}
          />
          <small>Leave empty to keep current image.</small>
        </div>
        
        <div className="grid">
          <button 
            type="button" 
            className="secondary" 
            onClick={onClose}
            disabled={isSubmitting}
          >
            Cancel
          </button>
          <button 
            type="submit" 
            aria-busy={isSubmitting}
            disabled={isSubmitting}
          >
            {isSubmitting ? 'Saving...' : 'Save Changes'}
          </button>
        </div>
      </form>
    </Modal>
  );
}

// New modal for creating items
function CreateItemModal({ isOpen, onClose, onSubmit, categoryName, isSubmitting }) {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [price, setPrice] = useState('');
  const [imageFile, setImageFile] = useState(null);
  const [localSubmitting, setLocalSubmitting] = useState(false);
  
  // Reset form when modal opens/closes
  useEffect(() => {
    if (!isOpen) {
      resetForm();
    }
  }, [isOpen]);
  
  const resetForm = () => {
    setName('');
    setDescription('');
    setPrice('');
    setImageFile(null);
  };
  
  const handleSubmit = async (e) => {
    e.preventDefault();
    
    if (!name) return;
    
    setLocalSubmitting(true);
    try {
      const formData = {
        name,
        description,
        price: parseFloat(price || 0)
      };
      
      if (imageFile) {
        const base64Str = await fileToBase64(imageFile);
        formData.image_data = base64Str;
      }
      
      await onSubmit(formData);
      resetForm();
    } catch (error) {
      console.error("Error creating item:", error);
    } finally {
      setLocalSubmitting(false);
    }
  };
  
  // Helper to convert file -> base64
  const fileToBase64 = (file) => {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(reader.result);
      reader.onerror = (error) => reject(error);
      reader.readAsDataURL(file);
    });
  };
  
  if (!isOpen) return null;
  
  const submitInProgress = isSubmitting || localSubmitting;
  
  return (
    <Modal isOpen={isOpen} onClose={onClose} title={`Add Item to ${categoryName}`}>
      <form onSubmit={handleSubmit}>
        <div className="grid">
          <label>
            <span className="required-field">Name</span>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Item Name"
              required
              disabled={submitInProgress}
            />
          </label>
          
          <label>
            <span className="required-field">Price</span>
            <input
              type="number"
              step="0.01"
              value={price}
              placeholder="0.00"
              onChange={(e) => setPrice(e.target.value)}
              required
              disabled={submitInProgress}
            />
          </label>
        </div>
        
        <label>
          Description <small>(optional)</small>
          <textarea
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            placeholder="Item description..."
            rows={3}
            disabled={submitInProgress}
          />
        </label>
        
        <div style={{ marginBottom: '1.5rem', marginTop: '1rem' }}>
          <label>
            Image <small>(optional)</small>
            <input
              type="file"
              accept="image/*"
              onChange={(e) => setImageFile(e.target.files[0])}
              disabled={submitInProgress}
            />
          </label>
        </div>
        
        <div className="grid">
          <button 
            type="button" 
            className="secondary" 
            onClick={onClose}
            disabled={submitInProgress}
          >
            Cancel
          </button>
          <button 
            type="submit" 
            aria-busy={submitInProgress}
            disabled={submitInProgress}
          >
            {submitInProgress ? 'Adding...' : 'Add Item'}
          </button>
        </div>
      </form>
    </Modal>
  );
}

export default MenusPage;
