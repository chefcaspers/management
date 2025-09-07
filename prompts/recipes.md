We want to generate some realistic data for various recipes that can be prepared in a kitchen.
Each recipe should have a list of ingredients and a list of instructions on how to prepare the dish.
The data should be in JSON format.

The collected recipe data should be formatted in JSON as the following <example>:

<example>
{
  "name": "Classic Burger",
  "description": "A classic burger with beef patty, lettuce, and other toppings",
  "category": "Fast Food",
  "price": 10.99,
  "image_url": "https://example.com/classic-burger.jpg",
  "ingredients": [
    {
      "name": "ingredients/beef",
      "quantity": "130g"
    },
    {
      "name": "ingredients/lettuce",
      "quantity": "50g"
    }
  ],
  "instructions": [
    {
      "step": "prepare",
      "required_station": "workstation",
      "expected_duration": "2 minutes",
      "description": "Prepare the ingredients"
    },
    {
      "step": "cook-patty",
      "required_station": "stove",
      "expected_duration": 10 minutes,
      "description": "Cook the beef patty on the stove until it's cooked through"
    },
    {
      "step": "assemble",
      "required_station": "workstation",
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
The following example shows an <example> of the JSON structure.

<example>
{
  "name": "ingredients/soy_sauce",
  "display_name": "Soy Sauce",
  "description": "Fermented soy and wheat sauce.",
  "nutritional_value": {
    "calories": "10 kcal/15ml",
    "protein": "1g/15ml",
    "fat": "0g/15ml",
    "carbohydrates": "1g/15ml"
  },
  "potential_allergens": ["soy", "wheat"]
}
</example>

The create fies per food category - like Asion, Mexican, Fast Food. Sepect a few popular categries.