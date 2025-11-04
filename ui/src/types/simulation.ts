export interface SimulationMetrics {
  totalCustomers: number;
  activeOrders: number;
  avgDeliveryTime: number;
  avgRating: number;
  ordersCompleted: number;
  kitchenUtilization: number;
}

export interface BrandPerformance {
  id: string;
  brand: string;
  vendor: string;
  activeOrders: number;
  completedOrders: number;
  rating: number;
  revenue: number;
}

export interface SimulationEvent {
  id: string;
  type: string;
  timestamp: Date;
  message: string;
  details?: Record<string, any>;
}

export interface SimulationParameters {
  customer: {
    arrivalRate: number;
    orderFrequency: number;
    appetiteVariation: number;
    priceNSensitivity: number;
  };
  kitchen: {
    preparationSpeed: number;
    errorRate: number;
    capacityUtilization: number;
    staffing: number;
  };
  delivery: {
    baseTime: number;
    trafficVariation: number;
    driverAvailability: number;
    weatherImpact: number;
  };
  feedback: {
    responseRate: number;
    satisfactionBias: number;
    negativeEventImpact: number;
    delayPenalty: number;
  };
}

export interface SimulationStatus {
  isRunning: boolean;
  startTime?: Date;
  elapsedTime: number;
}

