"""
Scrabble work
"""

import re
from collections import Counter

import pandas as pd

# Points and tile distribution
# '?' represents the blank tiles
POINTS_EN = {
    'A': 1, 'B': 3, 'C': 3, 'D': 2, 'E': 1, 'F': 4, 'G': 2, 'H': 4, 'I': 1,
    'J': 8, 'K': 5, 'L': 1, 'M': 3, 'N': 1, 'O': 1, 'P': 3, 'Q': 10, 'R': 1,
    'S': 1, 'T': 1, 'U': 1, 'V': 4, 'W': 4, 'X': 8, 'Y': 4, 'Z': 10, '?': 0,
}
DISTRIB_EN = {
    'A': 9, 'B': 2, 'C': 2, 'D': 4, 'E': 12, 'F': 2, 'G': 3, 'H': 2, 'I': 9,
    'J': 1, 'K': 1, 'L': 4, 'M': 2, 'N': 6, 'O': 8, 'P': 2, 'Q': 1, 'R': 6,
    'S': 4, 'T': 6, 'U': 4, 'V': 2, 'W': 2, 'X': 1, 'Y': 2, 'Z': 1, '?': 2,
}


class Loader:
    CSW_PATH = (
        r"C:\Program Files\NASPA Zyzzyva 3.4.1\data\words"
        r"\British\CSW21.txt"
    )
    NWL_PATH = (
        r"C:\Program Files\NASPA Zyzzyva 3.4.1\data\words"
        r"\North-American\NWL2023.txt"
    )
    CSV_PATH = (
        r"C:\Users\dgmat\Documents\Abel\scrabble_words.csv"
    )

    def __init__(self):
        """Initialize the Loader instance with empty attributes."""
        self.CSW = None
        self.NWL = None
        self.data = None

    def load_csw(self):
        """Load the CSW lexicon."""
        try:
            self.CSW = Scrabble.load_zyzzyva_lexicon(self.CSW_PATH)
        except Exception as e:
            print(f"Error loading CSW lexicon: {e}")
            self.CSW = None  # Explicitly return None to indicate failure
        return self.CSW

    def load_nwl(self):
        """Load the NWL lexicon."""
        try:
            self.NWL = Scrabble.load_zyzzyva_lexicon(self.NWL_PATH)
        except Exception as e:
            print(f"Error loading NWL lexicon: {e}")
            self.NWL = None  # Explicitly return None to indicate failure
        return self.NWL

    def load_csv(self):
        """Load the merged lexicon from a CSV file into a Scrabble object."""
        try:
            df = pd.read_csv(self.CSV_PATH)
            self.data = Scrabble(df)
        except FileNotFoundError:
            print(f"Error: The file {self.CSV_PATH} was not found.")
            self.data = None  # Return None in case of failure
        except Exception as e:
            print(f"Error loading merged lexicon from CSV: {e}")
            self.data = None
        return self.data

    def create_merged(self):
        """Create and return merged lexicon DataFrame with CSW and NWL."""
        if self.CSW is None or self.NWL is None:
            raise ValueError(
                "Both CSW and NWL lexicons must be loaded before merging."
            )
        self.data = Scrabble.merge_lexicons(self.CSW, self.NWL)
        return self.data

    def load_merged(self):
        """Load both CSW and NWL lexicons and merge them."""
        self.load_csw()  # Load CSW lexicon
        self.load_nwl()  # Load NWL lexicon
        return self.create_merged()  # Merge and return the lexicons

    def to_csv(self, df):
        """Save the provided DataFrame to a CSV file."""
        try:
            df.to_csv(self.CSV_PATH)
            print(f"Data saved to {self.CSV_PATH}")
        except Exception as e:
            print(f"Error saving to CSV: {e}")


class Scrabble(pd.DataFrame):
    _metadata = [
        "display_columns",
    ]

    def __init__(self, *args, display_columns=None, **kwargs):
        """
        Initialize the Scrabble DataFrame with optional display columns.
        If the first argument is an existing Scrabble instance,
        preserve its metadata.
        """
        super().__init__(*args, **kwargs)

        # Check if the first argument is a Scrabble instance and
        # preserve its metadata
        if args and isinstance(args[0], Scrabble):
            existing_df = args[0]
            if display_columns is None:
                display_columns = existing_df.display_columns

        # Set display_columns either from the passed argument
        # or the existing instance
        self.display_columns = display_columns

        # Automatically set 'word' as the index if it exists in the DataFrame
        if 'word' in self.columns:
            self.set_index('word', inplace=True)

    @property
    def _constructor(self):
        return Scrabble

    @classmethod
    def load_zyzzyva_lexicon(cls, filepath):
        """
        Load Zyzzyva lexicon into DataFrame with columns:
        word, definition, forms.
        """
        try:
            with open(filepath, 'r') as file:
                lines = file.readlines()

            data = []
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(' ', 1)
                word = parts[0].strip()
                definition = None
                forms_list = []

                if len(parts) > 1:
                    if '[' in parts[1] and ']' in parts[1]:
                        definition, forms = parts[1].rsplit('[', 1)
                        forms = forms.strip('[]')
                        forms_list = [
                            form.strip() for form in forms.split(',')
                        ]
                    else:
                        definition = parts[1].strip()
                data.append([word, definition, forms_list])

            words_df = pd.DataFrame(
                data, columns=['word', 'definition', 'forms']
            )
            return cls(words_df)
        except Exception as e:
            print(f"Error loading lexicon: {e}")
            return cls()

    @classmethod
    def merge_lexicons(cls, csw_lexicon, nwl_lexicon):
        """
        Merge CSW and NWL lexicons; add 'csw_only' and 'nwl_only' columns.
        Coalesce 'definition' columns into one.
        """
        merged_df = pd.merge(
            csw_lexicon, nwl_lexicon,
            how='outer',
            left_index=True,
            right_index=True,
            suffixes=('_csw', '_nwl'),
            indicator='merge_indicator'
        )

        # Coalesce the 'definition' columns
        merged_df['definition'] = merged_df['definition_csw'].combine_first(
            merged_df['definition_nwl']
        )

        # Add a column indicating if the word is exclusive to CSW or NWL
        merged_df['csw_only'] = merged_df['merge_indicator'] == 'left_only'
        merged_df['nwl_only'] = merged_df['merge_indicator'] == 'right_only'

        # Drop unnecessary columns
        merged_df.drop(
            columns=['definition_csw', 'definition_nwl', 'merge_indicator'],
            inplace=True
        )

        return cls(merged_df)

    def add_points(self):
        """Calculate and add Scrabble points for each word."""
        def calculate_points(word):
            return sum(POINTS_EN.get(char.upper(), 0) for char in word)

        self['points'] = self.index.map(calculate_points)

    def __repr__(self):
        """
        Display only selected columns if 'display_columns' is specified.
        Only the columns that exist in the DataFrame are displayed.
        """
        if self.display_columns:
            # Ensure that display_columns is a list
            self.display_columns = list(self.display_columns)
            # Ensure that the specified columns exist in the DataFrame
            available_columns = [
                col for col in self.display_columns if col in self.columns
            ]
            if available_columns:
                return super(Scrabble, self[available_columns]).__repr__()
            else:
                raise ValueError(
                    f"None of the columns in selected display column(s) "
                    f"{self.display_columns} are present in the data. "
                )
        else:
            return super().__repr__()

    def match(self, pattern):
        """
        Match rows using a regex, a 'Scrabble regex' (underscores), or a letter
        rack with '?' as blanks.

        Parameters:
        - pattern (str): A regex, 'Scrabble regex', or rack of letters.

        Returns:
        - Scrabble: DataFrame with filtered words.

        Examples:
        1. **Regex Match:**
           Use a regex pattern to match words that start with 'A' and end with 'Z':
           >>> regex_df = scrabble_df.match(r'^A.*Z$')

           This matches words like "AMAZE" or "ABUZZ".

        2. **Scrabble Regex (Fixed Positions):**
           Use underscores (`_`) to represent fixed positions:
           >>> fixed_df = scrabble_df.match('__O_E_O__')

           Finds 9 letter words where 'O' is at the 3rd position and so on,
           such as "WHOLESOME" or "CLOSEDOWN".

        3. **Rack Match (Anagram Search):**
           Match words that can be formed using the rack, including '?' as blanks:
           >>> rack_df = scrabble_df.match('LALLA??')

           Finds words like "ALLAY" or "ALARM" using 'L', 'A', 'L', 'L', and two blanks.
        """
        try:
            re.compile(pattern)
        except re.error:
            pass

        if '_' in pattern:
            pattern = pattern.replace('_', '.{1}').upper()
            pattern = f"^{pattern}$"
        else:
            return self.match_rack(pattern)

        return self[self.index.str.match(pattern)]

    def match_rack(self, rack):
        """Match words using a rack of letters with '?' as blanks."""
        rack_counter = Counter(rack.upper())

        def can_form(word):
            word_counter = Counter(word.upper())
            deficit = word_counter - rack_counter
            return sum(deficit.values()) <= rack_counter.get('?', 0)

        return self[self.index.map(can_form)]
