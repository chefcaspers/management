We want to generate some realistic data for various recipes that can be prepared in a kitchen.
Each recipe should have a list of ingredients and a list of instructions on how to prepare the dish. The data should be in JSON format.

The collected recipe data should be formatted in JSON as the following <example>:

<example>
{
  "name": "Classic Burger",
  "description": "A classic burger with beef patty, lettuce, and other toppings",
  "category": "Fast Food",
  "ingredients": [
    {
      "name": "ingredients/beef",
      "display_name": "Beef Patty",
      "description": "Delicious beef patty",
      "quantity": "130g"
    },
    {
      "name": "ingredients/lettuce",
      "display_name": "Lettuce",
      "description": "Crunchy lettuce leaves",
      "quantity": "50g"
    }
  ],
  "instructions": [
    {
      "step": "prepare",
      "required_assets": "workstation",
      "expected_duration": "2 minutes",
      "description": "Prepare the ingredients"
    },
    {
      "step": "cook-patty",
      "required_assets": "stove",
      "expected_duration": 10 minutes,
      "description": "Cook the beef patty on the stove until it's cooked through"
    },
    {
      "step": "assemble",
      "required_assets": "workstation",
      "expected_duration": "2 minutes",
      "description": "Assemble the burger with the cooked patty, lettuce, and other toppings"
    }
  ]
}
</example>

We need to make sure that there is at least some overlap between the ingredients used across recipes.

In the end provide a single file containing all ingredients used across all recipes.
Additional details about each ingredient, including its nutritional value and potential allergens should be included.
Nutritional values will be in relative terms per unit measure.

The create fies per food category - like Asion, Mexican, Fast Food. Sepect a few popular categries.