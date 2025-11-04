import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { ScrollArea } from '@/components/ui/scroll-area';
import { Badge } from '@/components/ui/badge';
import { SimulationEvent } from '@/types/simulation';
import { useEffect, useRef } from 'react';

interface EventStreamPanelProps {
  events: SimulationEvent[];
}

export function EventStreamPanel({ events }: EventStreamPanelProps) {
  const scrollRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [events]);

  const getEventTypeColor = (type: string) => {
    const colors: Record<string, string> = {
      'order.created': 'bg-blue-500',
      'order.completed': 'bg-green-500',
      'order.cancelled': 'bg-red-500',
      'kitchen.started': 'bg-yellow-500',
      'kitchen.completed': 'bg-green-500',
      'delivery.started': 'bg-purple-500',
      'delivery.completed': 'bg-green-500',
      'customer.joined': 'bg-blue-500',
      'feedback.received': 'bg-orange-500',
    };
    return colors[type] || 'bg-gray-500';
  };

  const formatTime = (date: Date) => {
    return date.toLocaleTimeString('en-US', {
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
  };

  return (
    <Card className="h-full flex flex-col">
      <CardHeader>
        <CardTitle>Event Stream</CardTitle>
        <CardDescription>Real-time simulation events</CardDescription>
      </CardHeader>
      <CardContent className="flex-1 overflow-hidden">
        <ScrollArea className="h-[calc(100vh-12rem)] pr-4">
          <div className="space-y-2">
            {events.length === 0 ? (
              <p className="text-sm text-muted-foreground text-center py-8">
                No events yet. Start the simulation to see events.
              </p>
            ) : (
              events.map((event) => (
                <div
                  key={event.id}
                  className="border rounded-lg p-3 space-y-1 hover:bg-accent transition-colors"
                >
                  <div className="flex items-center justify-between gap-2">
                    <Badge variant="outline" className="text-xs">
                      <div className={`w-2 h-2 rounded-full mr-2 ${getEventTypeColor(event.type)}`} />
                      {event.type}
                    </Badge>
                    <span className="text-xs text-muted-foreground">
                      {formatTime(event.timestamp)}
                    </span>
                  </div>
                  <p className="text-sm">{event.message}</p>
                  {event.details && Object.keys(event.details).length > 0 && (
                    <div className="text-xs text-muted-foreground font-mono">
                      {JSON.stringify(event.details, null, 2)}
                    </div>
                  )}
                </div>
              ))
            )}
          </div>
        </ScrollArea>
      </CardContent>
    </Card>
  );
}

