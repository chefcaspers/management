use caspers_universe_server::main;

use crate::ServerArgs;
use crate::error::Result;

pub(super) async fn handle(args: ServerArgs) -> Result<()> {
    main().await;
    Ok(())
}
