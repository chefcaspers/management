mod h3;

#[macro_export]
macro_rules! make_udf_function {
    ($UDF:ty, $NAME:ident) => {
        #[allow(rustdoc::redundant_explicit_links)]
        #[doc = concat!("Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of ", stringify!($NAME))]
        pub fn $NAME() -> std::sync::Arc<datafusion::logical_expr::ScalarUDF> {
            // Singleton instance of the function
            static INSTANCE: std::sync::LazyLock<
                std::sync::Arc<datafusion::logical_expr::ScalarUDF>,
            > = std::sync::LazyLock::new(|| {
                std::sync::Arc::new(datafusion::logical_expr::ScalarUDF::new_from_impl(
                    <$UDF>::new(),
                ))
            });
            std::sync::Arc::clone(&INSTANCE)
        }
    };
}

make_udf_function!(h3::LongLatAsH3, h3_longlatash3);
