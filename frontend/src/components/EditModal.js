import React from 'react';
import Modal from './Modal';

/**
 * Reusable edit modal component
 * @param {boolean} isOpen - Whether the modal is visible
 * @param {function} onClose - Function to call when the modal is closed
 * @param {string} title - The modal title
 * @param {object} data - The data to edit
 * @param {object} fields - The fields configuration object
 * @param {function} onSubmit - Function to call when the form is submitted
 */
function EditModal({ isOpen, onClose, title, data, fields, onSubmit }) {
  const [formData, setFormData] = React.useState({});
  const [isSubmitting, setIsSubmitting] = React.useState(false);

  // Initialize form data when data changes
  React.useEffect(() => {
    if (data) {
      const initialData = {};
      Object.keys(fields).forEach(key => {
        initialData[key] = data[key] !== undefined ? data[key] : '';
      });
      setFormData(initialData);
    }
  }, [data, fields]);

  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: type === 'checkbox' ? checked : value
    }));
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);
    try {
      await onSubmit(formData);
      onClose();
    } catch (error) {
      console.error("Error submitting form:", error);
      alert("An error occurred. Please try again.");
    } finally {
      setIsSubmitting(false);
    }
  };

  if (!data) return null;

  return (
    <Modal isOpen={isOpen} onClose={onClose} title={title}>
      <form onSubmit={handleSubmit}>
        {Object.entries(fields).map(([key, field]) => (
          <div key={key} className="form-group">
            <label htmlFor={key}>
              {field.label}
              {field.required && <span className="required">*</span>}
            </label>
            
            {field.type === 'select' ? (
              <select
                id={key}
                name={key}
                value={formData[key] || ''}
                onChange={handleChange}
                required={field.required}
                disabled={isSubmitting || field.disabled}
              >
                <option value="">{field.placeholder || '-- Select --'}</option>
                {field.options?.map(option => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            ) : field.type === 'textarea' ? (
              <textarea
                id={key}
                name={key}
                value={formData[key] || ''}
                onChange={handleChange}
                placeholder={field.placeholder}
                required={field.required}
                disabled={isSubmitting || field.disabled}
                rows={field.rows || 3}
              />
            ) : (
              <input
                id={key}
                type={field.type || 'text'}
                name={key}
                value={formData[key] || ''}
                onChange={handleChange}
                placeholder={field.placeholder}
                required={field.required}
                disabled={isSubmitting || field.disabled}
              />
            )}
            
            {field.help && (
              <small className="form-text">{field.help}</small>
            )}
          </div>
        ))}
        
        <div className="grid" style={{ marginTop: '1rem' }}>
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

export default EditModal;
