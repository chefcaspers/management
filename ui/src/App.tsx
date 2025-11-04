import { useEffect, useState } from 'react';
import { ControlPanel } from '@/components/ControlPanel';
import { MetricsPanel } from '@/components/MetricsPanel';
import { BrandPerformanceTable } from '@/components/BrandPerformanceTable';
import { SimulationParametersPanel } from '@/components/SimulationParametersPanel';
import { EventStreamPanel } from '@/components/EventStreamPanel';
import {
  SimulationMetrics,
  BrandPerformance,
  SimulationEvent,
  SimulationParameters,
  SimulationStatus,
} from '@/types/simulation';

// Default parameters
const DEFAULT_PARAMETERS: SimulationParameters = {
  customer: {
    arrivalRate: 5.0,
    orderFrequency: 1.5,
    appetiteVariation: 30,
    priceNSensitivity: 50,
  },
  kitchen: {
    preparationSpeed: 100,
    errorRate: 2.0,
    capacityUtilization: 75,
    staffing: 100,
  },
  delivery: {
    baseTime: 25,
    trafficVariation: 30,
    driverAvailability: 90,
    weatherImpact: 20,
  },
  feedback: {
    responseRate: 60,
    satisfactionBias: 0.5,
    negativeEventImpact: 1.5,
    delayPenalty: 0.8,
  },
};

function App() {
  const [status, setStatus] = useState<SimulationStatus>({
    isRunning: false,
    elapsedTime: 0,
  });

  const [metrics, setMetrics] = useState<SimulationMetrics>({
    totalCustomers: 0,
    activeOrders: 0,
    avgDeliveryTime: 0,
    avgRating: 0,
    ordersCompleted: 0,
    kitchenUtilization: 0,
  });

  const [brands, setBrands] = useState<BrandPerformance[]>([]);
  const [events, setEvents] = useState<SimulationEvent[]>([]);
  const [parameters, setParameters] = useState<SimulationParameters>(DEFAULT_PARAMETERS);

  // Timer for elapsed time
  useEffect(() => {
    let interval: number | undefined;
    if (status.isRunning) {
      interval = window.setInterval(() => {
        setStatus((prev) => ({
          ...prev,
          elapsedTime: prev.elapsedTime + 1,
        }));
      }, 1000);
    }
    return () => {
      if (interval) clearInterval(interval);
    };
  }, [status.isRunning]);

  // Simulate metrics updates when running
  useEffect(() => {
    if (!status.isRunning) return;

    const interval = setInterval(() => {
      // Simulate metrics changes
      setMetrics((prev) => ({
        totalCustomers: prev.totalCustomers + Math.floor(Math.random() * 3),
        activeOrders: Math.max(0, prev.activeOrders + Math.floor(Math.random() * 5 - 2)),
        avgDeliveryTime: Math.max(15, Math.min(45, prev.avgDeliveryTime + (Math.random() - 0.5) * 2)),
        avgRating: Math.max(3, Math.min(5, prev.avgRating + (Math.random() - 0.5) * 0.1)),
        ordersCompleted: prev.ordersCompleted + Math.floor(Math.random() * 2),
        kitchenUtilization: Math.max(50, Math.min(100, prev.kitchenUtilization + (Math.random() - 0.5) * 5)),
      }));

      // Add random events
      const eventTypes = [
        'order.created',
        'order.completed',
        'kitchen.started',
        'delivery.started',
        'feedback.received',
      ];
      const randomType = eventTypes[Math.floor(Math.random() * eventTypes.length)];
      
      setEvents((prev) => [
        ...prev,
        {
          id: `event-${Date.now()}-${Math.random()}`,
          type: randomType,
          timestamp: new Date(),
          message: `Event ${randomType} occurred`,
          details: { orderId: Math.floor(Math.random() * 10000) },
        },
      ].slice(-100)); // Keep last 100 events
    }, 2000);

    return () => clearInterval(interval);
  }, [status.isRunning]);

  const handleStart = async () => {
    // TODO: Call API to start simulation with current parameters
    console.log('Starting simulation with parameters:', parameters);
    
    setStatus({
      isRunning: true,
      startTime: new Date(),
      elapsedTime: 0,
    });

    // Initialize with some sample brand data
    setBrands([
      {
        id: '1',
        brand: 'Burger Palace',
        vendor: 'FastFood Inc',
        activeOrders: 12,
        completedOrders: 543,
        rating: 4.5,
        revenue: 12345,
      },
      {
        id: '2',
        brand: 'Taco Express',
        vendor: 'Mexican Delights',
        activeOrders: 8,
        completedOrders: 412,
        rating: 4.3,
        revenue: 9876,
      },
      {
        id: '3',
        brand: 'Sushi Station',
        vendor: 'Asian Fusion',
        activeOrders: 15,
        completedOrders: 678,
        rating: 4.7,
        revenue: 15432,
      },
    ]);

    setMetrics({
      totalCustomers: 1250,
      activeOrders: 35,
      avgDeliveryTime: 28.5,
      avgRating: 4.5,
      ordersCompleted: 1633,
      kitchenUtilization: 78.5,
    });
  };

  const handleStop = async () => {
    // TODO: Call API to stop simulation
    console.log('Stopping simulation');
    setStatus((prev) => ({ ...prev, isRunning: false }));
  };

  const handleReset = () => {
    // Reset to default parameters
    setParameters(DEFAULT_PARAMETERS);
    console.log('Reset to default parameters');
  };

  return (
    <div className="min-h-screen bg-background">
      <div className="border-b">
        <div className="container mx-auto px-4 py-4">
          <h1 className="text-3xl font-bold">Chef Casper's Universe</h1>
          <p className="text-muted-foreground">
            Ghost Kitchen Management Simulation Dashboard
          </p>
        </div>
      </div>

      <div className="container mx-auto px-4 py-6">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Main Content Area - Left Side (2/3) */}
          <div className="lg:col-span-2 space-y-6">
            <ControlPanel
              status={status}
              onStart={handleStart}
              onStop={handleStop}
              onReset={handleReset}
            />
            
            <MetricsPanel metrics={metrics} />
            
            <BrandPerformanceTable brands={brands} />
            
            <SimulationParametersPanel
              parameters={parameters}
              onChange={setParameters}
            />
          </div>

          {/* Event Stream - Right Side (1/3) */}
          <div className="lg:col-span-1">
            <div className="sticky top-6">
              <EventStreamPanel events={events} />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
