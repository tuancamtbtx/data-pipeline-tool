import re


class MacroRenderer:
    def __init__(self, macros=None):
        """
        Initialize the MacroRenderer with an optional dictionary of macros.

        :param macros: A dictionary where keys are macro names and values are their replacements.
        """
        self.macros = macros if macros is not None else {}

    def set_macro(self, name, value):
        """
        Set a macro with a given name and value.

        :param name: The name of the macro.
        :param value: The value to replace the macro with.
        """
        self.macros[name] = value

    def render(self, input_string):
        """
        Render the input string by replacing macros in the form of {{macro_name}} with their values.

        :param input_string: The input string containing macros.
        :return: The rendered string with macros replaced by their values.
        """

        def replace_macro(match):
            macro_name = match.group(1)
            return self.macros.get(macro_name, f"{{{{{macro_name}}}}}")

        # Use regular expression to find and replace macros
        pattern = re.compile(r"\{\{(\w+)\}\}")
        return pattern.sub(replace_macro, input_string)
