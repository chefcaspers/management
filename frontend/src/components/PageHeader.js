import React from 'react';

/**
 * A consistent header component for all admin pages
 * @param {string} title - The page title
 * @param {string} description - Optional description text
 */
function PageHeader({ title, description }) {
  return (
    <header>
      <hgroup>
        <h2>{title}</h2>
        {description && <h3>{description}</h3>}
      </hgroup>
    </header>
  );
}

export default PageHeader;
