import React from 'react';

/**
 * Reusable table component with Pico styling
 * @param {Object[]} columns - Array of column definitions with 'header' and 'key' properties
 * @param {Object[]} data - Array of data objects to display
 * @param {Function} renderCell - Custom cell renderer function (optional)
 */
function Table({ columns, data, renderCell }) {
  if (!data || data.length === 0) {
    return <p><em>No data available</em></p>;
  }

  return (
    <figure>
      <table role="grid">
        <thead>
          <tr>
            {columns.map(col => (
              <th key={col.key}>{col.header}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, rowIndex) => (
            <tr key={row.id || rowIndex}>
              {columns.map(col => (
                <td key={`${row.id || rowIndex}-${col.key}`}>
                  {renderCell ? renderCell(row, col.key) : row[col.key]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </figure>
  );
}

export default Table;
