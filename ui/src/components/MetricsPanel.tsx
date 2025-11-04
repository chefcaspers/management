import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { SimulationMetrics } from '@/types/simulation';
import { Users, ShoppingCart, Clock, Star, CheckCircle, Activity } from 'lucide-react';

interface MetricsPanelProps {
  metrics: SimulationMetrics;
}

export function MetricsPanel({ metrics }: MetricsPanelProps) {
  const metricCards = [
    {
      title: "Total Customers",
      value: metrics.totalCustomers.toLocaleString(),
      icon: Users,
      color: "text-blue-500"
    },
    {
      title: "Active Orders",
      value: metrics.activeOrders.toLocaleString(),
      icon: ShoppingCart,
      color: "text-green-500"
    },
    {
      title: "Avg Delivery Time",
      value: `${metrics.avgDeliveryTime.toFixed(1)} min`,
      icon: Clock,
      color: "text-orange-500"
    },
    {
      title: "Avg Rating",
      value: metrics.avgRating.toFixed(2),
      icon: Star,
      color: "text-yellow-500"
    },
    {
      title: "Orders Completed",
      value: metrics.ordersCompleted.toLocaleString(),
      icon: CheckCircle,
      color: "text-purple-500"
    },
    {
      title: "Kitchen Utilization",
      value: `${metrics.kitchenUtilization.toFixed(1)}%`,
      icon: Activity,
      color: "text-red-500"
    }
  ];

  return (
    <Card>
      <CardHeader>
        <CardTitle>Metrics Overview</CardTitle>
        <CardDescription>Real-time simulation metrics</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
          {metricCards.map((metric) => (
            <div key={metric.title} className="space-y-2">
              <div className="flex items-center gap-2">
                <metric.icon className={`h-4 w-4 ${metric.color}`} />
                <p className="text-sm font-medium text-muted-foreground">
                  {metric.title}
                </p>
              </div>
              <p className="text-2xl font-bold">{metric.value}</p>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

