import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock


for module_name in (
    "cryptography",
    "cryptography.hazmat",
    "cryptography.hazmat.primitives",
    "cryptography.hazmat.primitives.asymmetric",
    "cryptography.hazmat.primitives.asymmetric.padding",
):
    sys.modules.setdefault(module_name, MagicMock())


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "chief_fafa_recipe_pipeline.py"
SPEC = importlib.util.spec_from_file_location("chief_fafa_recipe_pipeline", MODULE_PATH)
pipeline = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(pipeline)


class RecipeTitleTests(unittest.TestCase):
    def test_rejects_non_recipe_titles_seen_in_google_docs(self):
        weak_titles = [
            "約1湯",
            "高筋麵粉",
            "@the_sasaya",
            "米粉",
            "糯米粉",
            "potato starch",
            "찹쌀가루",
            "薄力粉",
            "마스카포네 크림 마스카포네 크림...",
            "樹薯粉",
        ]
        for title in weak_titles:
            with self.subTest(title=title):
                self.assertTrue(pipeline.title_looks_generic(title))

    def test_accepts_specific_finished_dish_titles(self):
        good_titles = [
            "Matcha Warabi Mochi",
            "黑芝麻麻糬",
            "Mascarpone Cream Cake",
            "마스카포네 크림 케이크",
        ]
        for title in good_titles:
            with self.subTest(title=title):
                self.assertFalse(pipeline.title_looks_generic(title))

    def test_prefers_specific_dish_over_ingredient_or_handle(self):
        self.assertEqual(
            pipeline.choose_better_recipe_title("高筋麵粉", "日式牛奶吐司"),
            "日式牛奶吐司",
        )
        self.assertEqual(
            pipeline.choose_better_recipe_title("@the_sasaya", "Matcha Warabi Mochi"),
            "Matcha Warabi Mochi",
        )

    def test_fallback_context_can_rescue_social_handle(self):
        self.assertEqual(
            pipeline.short_recipe_title(
                "@the_sasaya",
                fallback_text="Matcha Warabi Mochi\nA soft matcha dessert made with tapioca starch.",
            ),
            "Matcha Warabi Mochi",
        )

    def test_removes_recipe_call_to_action_suffix(self):
        self.assertEqual(
            pipeline.short_recipe_title("濃厚マロンペーストのレシピはこちら"),
            "濃厚マロンペースト",
        )

    def test_semantically_deduplicates_shifted_ingredients(self):
        merged = pipeline.merge_ingredient_lines(
            ["75g tapioca starch", "50g sugar", "2g matcha powder", "250g water"],
            ["g tapioca starch 50g", "sugar 2g", "matcha powder 250g"],
        )
        self.assertEqual(
            merged,
            ["75g tapioca starch", "50g sugar", "2g matcha powder", "250g water"],
        )


if __name__ == "__main__":
    unittest.main()
