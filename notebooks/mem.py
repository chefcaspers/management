import polars as pl
import duckdb
import daft
from deltalake import DeltaTable
import pyarrow as pa

df_arrow = DeltaTable("notebooks/data/orders_delta").to_pyarrow_table()
df_polars = pl.from_arrow(df_arrow)
df_pandas = df_arrow.to_pandas()
df_daft = daft.from_arrow(df_arrow)

arrow_again = duckdb.sql("select * from df_arrow").arrow()
arrow_again = df_polars.to_arrow()
arrow_again = df_daft.to_arrow()
arrow_again = pa.Table.from_pandas(df_pandas)

print(df_polars.shape)
print(df_arrow.shape)
print(df_pandas.shape)
print(arrow_again.shape)
print(df_daft.schema())
