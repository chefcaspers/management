# Chef Casper's Universe UI

A modern web interface for the Ghost Kitchen Management Simulation built with:

- **React** - UI framework
- **TypeScript** - Type safety
- **Vite** - Build tool and dev server
- **Shadcn UI** - Component library
- **Tailwind CSS** - Styling

## Getting Started

### Install Dependencies

```bash
npm install
```

### Development

```bash
npm run dev
```

The dev server will start on `http://localhost:5173` with API proxy to the backend server.

### Build

```bash
npm run build
```

Outputs to `dist/` directory, which is served by the Axum server.

### Preview Production Build

```bash
npm run preview
```

## Adding Shadcn UI Components

Use the Shadcn CLI to add components:

```bash
npx shadcn@latest add button
npx shadcn@latest add card
npx shadcn@latest add dialog
# ... etc
```

Components will be added to `src/components/ui/`.

## Project Structure

```
ui/
├── src/
│   ├── components/      # React components
│   │   └── ui/          # Shadcn UI components
│   ├── lib/             # Utility functions
│   ├── App.tsx          # Main app component
│   ├── main.tsx         # Entry point
│   └── index.css        # Global styles with Tailwind
├── public/              # Static assets
├── index.html           # HTML template
└── vite.config.ts       # Vite configuration
```

