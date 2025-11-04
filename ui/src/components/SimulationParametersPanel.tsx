import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Label } from '@/components/ui/label';
import { Slider } from '@/components/ui/slider';
import { SimulationParameters } from '@/types/simulation';

interface SimulationParametersPanelProps {
  parameters: SimulationParameters;
  onChange: (parameters: SimulationParameters) => void;
}

export function SimulationParametersPanel({ parameters, onChange }: SimulationParametersPanelProps) {
  const updateParameter = <T extends keyof SimulationParameters>(
    category: T,
    key: keyof SimulationParameters[T],
    value: number
  ) => {
    onChange({
      ...parameters,
      [category]: {
        ...parameters[category],
        [key]: value,
      },
    });
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Simulation Parameters</CardTitle>
        <CardDescription>Adjust simulation behavior in real-time</CardDescription>
      </CardHeader>
      <CardContent>
        <Tabs defaultValue="customer" className="w-full">
          <TabsList className="grid w-full grid-cols-4">
            <TabsTrigger value="customer">Customer</TabsTrigger>
            <TabsTrigger value="kitchen">Kitchen</TabsTrigger>
            <TabsTrigger value="delivery">Delivery</TabsTrigger>
            <TabsTrigger value="feedback">Feedback</TabsTrigger>
          </TabsList>

          <TabsContent value="customer" className="space-y-4">
            <div className="space-y-2">
              <Label>Arrival Rate: {parameters.customer.arrivalRate.toFixed(1)} customers/min</Label>
              <Slider
                value={[parameters.customer.arrivalRate]}
                onValueChange={(value) => updateParameter('customer', 'arrivalRate', value[0])}
                min={0}
                max={20}
                step={0.5}
              />
            </div>
            <div className="space-y-2">
              <Label>Order Frequency: {parameters.customer.orderFrequency.toFixed(1)} orders/day</Label>
              <Slider
                value={[parameters.customer.orderFrequency]}
                onValueChange={(value) => updateParameter('customer', 'orderFrequency', value[0])}
                min={0}
                max={5}
                step={0.1}
              />
            </div>
            <div className="space-y-2">
              <Label>Appetite Variation: {parameters.customer.appetiteVariation.toFixed(0)}%</Label>
              <Slider
                value={[parameters.customer.appetiteVariation]}
                onValueChange={(value) => updateParameter('customer', 'appetiteVariation', value[0])}
                min={0}
                max={100}
                step={5}
              />
            </div>
            <div className="space-y-2">
              <Label>Price Sensitivity: {parameters.customer.priceNSensitivity.toFixed(0)}%</Label>
              <Slider
                value={[parameters.customer.priceNSensitivity]}
                onValueChange={(value) => updateParameter('customer', 'priceNSensitivity', value[0])}
                min={0}
                max={100}
                step={5}
              />
            </div>
          </TabsContent>

          <TabsContent value="kitchen" className="space-y-4">
            <div className="space-y-2">
              <Label>Preparation Speed: {parameters.kitchen.preparationSpeed.toFixed(0)}%</Label>
              <Slider
                value={[parameters.kitchen.preparationSpeed]}
                onValueChange={(value) => updateParameter('kitchen', 'preparationSpeed', value[0])}
                min={50}
                max={200}
                step={5}
              />
            </div>
            <div className="space-y-2">
              <Label>Error Rate: {parameters.kitchen.errorRate.toFixed(1)}%</Label>
              <Slider
                value={[parameters.kitchen.errorRate]}
                onValueChange={(value) => updateParameter('kitchen', 'errorRate', value[0])}
                min={0}
                max={20}
                step={0.5}
              />
            </div>
            <div className="space-y-2">
              <Label>Capacity Utilization: {parameters.kitchen.capacityUtilization.toFixed(0)}%</Label>
              <Slider
                value={[parameters.kitchen.capacityUtilization]}
                onValueChange={(value) => updateParameter('kitchen', 'capacityUtilization', value[0])}
                min={0}
                max={100}
                step={5}
              />
            </div>
            <div className="space-y-2">
              <Label>Staffing Level: {parameters.kitchen.staffing.toFixed(0)}%</Label>
              <Slider
                value={[parameters.kitchen.staffing]}
                onValueChange={(value) => updateParameter('kitchen', 'staffing', value[0])}
                min={50}
                max={150}
                step={5}
              />
            </div>
          </TabsContent>

          <TabsContent value="delivery" className="space-y-4">
            <div className="space-y-2">
              <Label>Base Time: {parameters.delivery.baseTime.toFixed(0)} min</Label>
              <Slider
                value={[parameters.delivery.baseTime]}
                onValueChange={(value) => updateParameter('delivery', 'baseTime', value[0])}
                min={10}
                max={60}
                step={1}
              />
            </div>
            <div className="space-y-2">
              <Label>Traffic Variation: {parameters.delivery.trafficVariation.toFixed(0)}%</Label>
              <Slider
                value={[parameters.delivery.trafficVariation]}
                onValueChange={(value) => updateParameter('delivery', 'trafficVariation', value[0])}
                min={0}
                max={100}
                step={5}
              />
            </div>
            <div className="space-y-2">
              <Label>Driver Availability: {parameters.delivery.driverAvailability.toFixed(0)}%</Label>
              <Slider
                value={[parameters.delivery.driverAvailability]}
                onValueChange={(value) => updateParameter('delivery', 'driverAvailability', value[0])}
                min={50}
                max={100}
                step={5}
              />
            </div>
            <div className="space-y-2">
              <Label>Weather Impact: {parameters.delivery.weatherImpact.toFixed(0)}%</Label>
              <Slider
                value={[parameters.delivery.weatherImpact]}
                onValueChange={(value) => updateParameter('delivery', 'weatherImpact', value[0])}
                min={0}
                max={100}
                step={5}
              />
            </div>
          </TabsContent>

          <TabsContent value="feedback" className="space-y-4">
            <div className="space-y-2">
              <Label>Response Rate: {parameters.feedback.responseRate.toFixed(0)}%</Label>
              <Slider
                value={[parameters.feedback.responseRate]}
                onValueChange={(value) => updateParameter('feedback', 'responseRate', value[0])}
                min={0}
                max={100}
                step={5}
              />
            </div>
            <div className="space-y-2">
              <Label>Satisfaction Bias: {parameters.feedback.satisfactionBias.toFixed(1)}</Label>
              <Slider
                value={[parameters.feedback.satisfactionBias]}
                onValueChange={(value) => updateParameter('feedback', 'satisfactionBias', value[0])}
                min={-2}
                max={2}
                step={0.1}
              />
            </div>
            <div className="space-y-2">
              <Label>Negative Event Impact: {parameters.feedback.negativeEventImpact.toFixed(1)}</Label>
              <Slider
                value={[parameters.feedback.negativeEventImpact]}
                onValueChange={(value) => updateParameter('feedback', 'negativeEventImpact', value[0])}
                min={0}
                max={5}
                step={0.1}
              />
            </div>
            <div className="space-y-2">
              <Label>Delay Penalty: {parameters.feedback.delayPenalty.toFixed(1)}</Label>
              <Slider
                value={[parameters.feedback.delayPenalty]}
                onValueChange={(value) => updateParameter('feedback', 'delayPenalty', value[0])}
                min={0}
                max={3}
                step={0.1}
              />
            </div>
          </TabsContent>
        </Tabs>
      </CardContent>
    </Card>
  );
}

