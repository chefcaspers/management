import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { BrandPerformance } from '@/types/simulation';
import { Badge } from '@/components/ui/badge';

interface BrandPerformanceTableProps {
  brands: BrandPerformance[];
}

export function BrandPerformanceTable({ brands }: BrandPerformanceTableProps) {
  const formatCurrency = (amount: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(amount);
  };

  const getRatingColor = (rating: number) => {
    if (rating >= 4.5) return 'default';
    if (rating >= 4.0) return 'secondary';
    if (rating >= 3.5) return 'outline';
    return 'destructive';
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Brand Performance</CardTitle>
        <CardDescription>Performance metrics by brand</CardDescription>
      </CardHeader>
      <CardContent>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Brand</TableHead>
              <TableHead>Vendor</TableHead>
              <TableHead className="text-right">Active Orders</TableHead>
              <TableHead className="text-right">Completed Orders</TableHead>
              <TableHead className="text-right">Rating</TableHead>
              <TableHead className="text-right">Revenue</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {brands.map((brand) => (
              <TableRow key={brand.id}>
                <TableCell className="font-medium">{brand.brand}</TableCell>
                <TableCell className="text-muted-foreground">{brand.vendor}</TableCell>
                <TableCell className="text-right">{brand.activeOrders}</TableCell>
                <TableCell className="text-right">{brand.completedOrders}</TableCell>
                <TableCell className="text-right">
                  <Badge variant={getRatingColor(brand.rating)}>
                    {brand.rating.toFixed(2)}
                  </Badge>
                </TableCell>
                <TableCell className="text-right font-semibold">
                  {formatCurrency(brand.revenue)}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

