import { Button } from '@/components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Play, Pause, RotateCcw } from 'lucide-react';
import { SimulationStatus } from '@/types/simulation';

interface ControlPanelProps {
  status: SimulationStatus;
  onStart: () => void;
  onStop: () => void;
  onReset: () => void;
}

export function ControlPanel({ status, onStart, onStop, onReset }: ControlPanelProps) {
  const formatElapsedTime = (seconds: number) => {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
  };

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle>Simulation Control</CardTitle>
            <CardDescription>Manage simulation execution</CardDescription>
          </div>
          <Badge variant={status.isRunning ? "default" : "secondary"}>
            {status.isRunning ? "Running" : "Stopped"}
          </Badge>
        </div>
      </CardHeader>
      <CardContent>
        <div className="space-y-4">
          <div className="flex items-center gap-4">
            {!status.isRunning ? (
              <Button onClick={onStart} className="flex-1">
                <Play className="mr-2 h-4 w-4" />
                Start Simulation
              </Button>
            ) : (
              <Button onClick={onStop} variant="destructive" className="flex-1">
                <Pause className="mr-2 h-4 w-4" />
                Stop Simulation
              </Button>
            )}
            <Button onClick={onReset} variant="outline">
              <RotateCcw className="mr-2 h-4 w-4" />
              Reset to Default
            </Button>
          </div>
          {status.isRunning && (
            <div className="text-sm">
              <p className="text-muted-foreground">Elapsed Time</p>
              <p className="text-2xl font-mono font-semibold">
                {formatElapsedTime(status.elapsedTime)}
              </p>
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

