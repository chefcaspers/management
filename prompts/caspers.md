We are building an agent based simulator to model the behavior of restaurants and their customers.
The simulator will allow us to explore various scenarios and gain insights into the dynamics of
restaurant operations. We are the operators of a "Ghost Kitchen" - kitchen management system.
Here Vendors can rent kitchens to prepare the meals they offer.

We as operators offer the physical space as well as the kitchen / order management system.
We also provide a comprehensive suite of tools to help vendors manage their operations,
including inventory tracking, order fulfillment, and customer feedback. Our platform is
designed to streamline the entire restaurant operation process, from menu creation to customer service.

We want the simulator to generate realistic data by simulating interactions and introducing
randomness to create a more dynamic environment.

Before we go into the details, we need to set up the basic simulation framework. This involves
defining the classes and data structures that will represent the entities in our simulation,
such as Locations, Vendors, Brands, Kitchens, Menus, Categories, and Items. We will also need
to establish the relationships between these entities and define the rules that govern their behavior.
Lets begin by defining the root abstractions / traits we require to build the simulation.
