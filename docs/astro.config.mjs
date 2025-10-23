// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

// https://astro.build/config
export default defineConfig({
	integrations: [
		starlight({
			title: 'Caspers\' Universe Docs',
			social: [{ icon: 'github', label: 'GitHub', href: 'ttps://github.com/chefcaspers/management' }],
			sidebar: [
  			{
  				label: 'Tutorials',
  				items: [
  					{ label: 'Getting Started', slug: 'tutorials/getting-started' },
  				],
  			},
			  {
					label: 'Guides',
					items: [
						{ label: 'Getting Started', slug: 'guides/getting-started' },
					],
				},
				{
					label: 'Reference',
					autogenerate: { directory: 'reference' },
				},
			],
		}),
	],
});
