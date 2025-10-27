use caspers_universe::{BrandTemplate, SiteTemplate, Template, initialize_template, resolve_url};
use dialoguer::MultiSelect;

use crate::error::Result;

#[derive(Debug, Clone, clap::Parser)]
pub(super) struct InitArgs {
    #[arg(short, long, default_value_t = true)]
    template: bool,

    #[arg(short, long)]
    working_directory: Option<String>,
}

pub(super) async fn handle(args: InitArgs) -> Result<()> {
    let caspers_directory = resolve_url(args.working_directory)?;
    if args.template {
        let sites = vec![
            SiteTemplate::Amsterdam,
            SiteTemplate::Berlin,
            SiteTemplate::London,
        ];
        let brands = vec![
            BrandTemplate::Asian,
            BrandTemplate::FastFood,
            BrandTemplate::Mexican,
        ];

        let Some(site_selection) = MultiSelect::new()
            .with_prompt("Which sites should be included?")
            .items(&sites)
            .defaults(&[true, false, true])
            .interact_opt()?
        else {
            return Ok(());
        };

        let Some(brand_selection) = MultiSelect::new()
            .with_prompt("Which brands should be included?")
            .items(&brands)
            .defaults(&[true, true, true])
            .interact_opt()?
        else {
            return Ok(());
        };

        let selected_sites = site_selection
            .into_iter()
            .map(|idx| sites[idx])
            .collect::<Vec<_>>();

        let selected_brands = brand_selection
            .into_iter()
            .map(|idx| brands[idx])
            .collect::<Vec<_>>();

        let template = Template::new(selected_sites, selected_brands);

        initialize_template(&caspers_directory, template).await?;

        println!("Template loaded successfully");
    } else {
        println!("Initializing without template");
    }
    Ok(())
}
