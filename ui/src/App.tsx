import { useEffect, useState } from 'react'

interface HealthCheck {
  status: string
  service: string
}

function App() {
  const [health, setHealth] = useState<HealthCheck | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetch('/api/health')
      .then(res => res.json())
      .then(data => {
        setHealth(data)
        setLoading(false)
      })
      .catch(err => {
        console.error('Failed to fetch health check:', err)
        setLoading(false)
      })
  }, [])

  return (
    <div className="min-h-screen bg-background text-foreground">
      <div className="container mx-auto p-8">
        <h1 className="text-4xl font-bold mb-4">Chef Casper's Universe</h1>
        <p className="text-muted-foreground mb-8">
          Ghost Kitchen Management Simulation
        </p>
        
        <div className="border rounded-lg p-6">
          <h2 className="text-2xl font-semibold mb-4">Server Status</h2>
          {loading ? (
            <p>Loading...</p>
          ) : health ? (
            <div className="space-y-2">
              <p><span className="font-medium">Status:</span> {health.status}</p>
              <p><span className="font-medium">Service:</span> {health.service}</p>
            </div>
          ) : (
            <p className="text-destructive">Failed to connect to server</p>
          )}
        </div>
      </div>
    </div>
  )
}

export default App

