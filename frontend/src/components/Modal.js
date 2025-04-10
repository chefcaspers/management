import React from 'react';

/**
 * Reusable modal component with Pico styling
 * @param {boolean} isOpen - Whether the modal is visible
 * @param {function} onClose - Function to call when the modal is closed
 * @param {string} title - The modal title
 * @param {React.ReactNode} children - The modal content
 */
function Modal({ isOpen, onClose, title, children }) {
  if (!isOpen) return null;

  return (
    <dialog open>
      <article>
        <header>
          <a href="#close" 
            aria-label="Close" 
            className="close" 
            onClick={(e) => {
              e.preventDefault();
              onClose();
            }}
          ></a>
          <h3>{title}</h3>
        </header>
        <div>{children}</div>
      </article>
    </dialog>
  );
}

export default Modal;
