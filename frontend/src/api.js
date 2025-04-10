// src/api.js
import axios from 'axios';

// -- LOCATIONS --
export async function getLocations() {
  return axios.get('/api/locations');
}

export async function createLocation({ name, address }) {
  return axios.post('/api/locations', { name, address });
}

export async function deleteLocation(id) {
  return axios.delete(`/api/locations/${id}`);
}

export async function updateLocation(id, updates) {
  return axios.put(`/api/locations/${id}`, updates);
}

// -- VENDORS --
export async function getVendors() {
  return axios.get('/api/vendors');
}

export async function createVendor({ name }) {
  return axios.post('/api/vendors', { name });
}

export async function deleteVendor(id) {
  return axios.delete(`/api/vendors/${id}`);
}

export async function updateVendor(id, updates) {
  return axios.put(`/api/vendors/${id}`, updates);
}

// -- KITCHENS --
export async function getKitchens() {
  return axios.get('/api/kitchens');
}

export async function createKitchen({ name, location_id, vendor_id }) {
  return axios.post('/api/kitchens', {
    name,
    location_id,
    vendor_id: vendor_id || null
  });
}

export async function deleteKitchen(id) {
  return axios.delete(`/api/kitchens/${id}`);
}

export async function updateKitchen(kitchenId, updates) {
  return axios.put(`/api/kitchens/${kitchenId}`, updates);
}

// -- BRANDS --
export async function getBrands() {
  return axios.get('/api/brands');
}

export async function createBrand({ name, vendor_id }) {
  return axios.post('/api/brands', { name, vendor_id });
}

export async function deleteBrand(id) {
  return axios.delete(`/api/brands/${id}`);
}

export async function updateBrand(id, updates) {
  return axios.put(`/api/brands/${id}`, updates);
}

export async function associateBrandWithKitchens(brandId, kitchenIds) {
  return axios.post(`/api/brands/${brandId}/kitchens`, {
    kitchen_ids: kitchenIds
  });
}

// -- MENUS --
export async function getMenus() {
  return axios.get('/api/menus');
}

// -- CATEGORIES --
export async function getCategories() {
  return axios.get('/api/categories');
}

export async function createCategory({ name, menu_id }) {
  return axios.post('/api/categories', { name, menu_id });
}

export async function deleteCategory(id) {
  return axios.delete(`/api/categories/${id}`);
}

export async function updateCategory(id, updates) {
  return axios.put(`/api/categories/${id}`, updates);
}

// -- ITEMS --
export async function getItems() {
  return axios.get('/api/items');
}

export async function createItem({ name, description, price, image_data, category_id }) {
  return axios.post('/api/items', {
    name,
    description,
    price,
    image_data,
    category_id
  });
}

export async function updateItem(id, updates) {
  return axios.put(`/api/items/${id}`, updates);
}

export async function deleteItem(id) {
  return axios.delete(`/api/items/${id}`);
}
