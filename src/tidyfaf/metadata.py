import pandas as pd
from pathlib import Path

class FAFMetadata:
    """
    Class to handle FAF metadata and code mappings.
    """
    def __init__(self, data_dir=None):
        if data_dir is None:
            self.data_dir = Path(__file__).resolve().parent.parent.parent / "data"
        else:
            self.data_dir = Path(data_dir)
            
        self.filename = "FAF5_metadata.xlsx"
        self.filepath = self.data_dir / self.filename
        
        if not self.filepath.exists():
            raise FileNotFoundError(f"FAF Metadata not found at {self.filepath}. Please run download.py first.")
            
        print(f"Loading FAF Metadata from {self.filepath}...")
        self.xl = pd.ExcelFile(self.filepath)
        
        # Cache for loaded sheets
        self._states = None
        self._modes = None
        self._commodities = None
        self._zones = None
        
    def _load_sheet(self, sheet_name):
        return pd.read_excel(self.xl, sheet_name=sheet_name)

    def get_table(self, sheet_name):
        """Returns the DataFrame for a specific sheet."""
        return self._load_sheet(sheet_name)

    @property
    def states(self):
        if self._states is None:
            # Sheet name is 'State'
            self._states = self._load_sheet('State')
        return self._states

    @property
    def modes(self):
        if self._modes is None:
            # Sheet name is 'Mode'
            self._modes = self._load_sheet('Mode')
        return self._modes

    @property
    def commodities(self):
        if self._commodities is None:
            # Sheet name is 'Commodity (SCTG2)'
            self._commodities = self._load_sheet('Commodity (SCTG2)')
        return self._commodities

    @property
    def zones(self):
        if self._zones is None:
            # Sheet name is 'FAF Zone (Domestic)'
            self._zones = self._load_sheet('FAF Zone (Domestic)')
        return self._zones

    def lookup_state(self, name):
        """
        Look up state code by name.
        Returns the numeric code.
        """
        df = self.states
        # Assuming columns are 'Code' and 'Description' or similar. 
        # Let's inspect first row to be sure, but usually it's standard.
        # Based on typical FAF metadata: 'Code', 'State Name' (or similar)
        # We will try to find the column that contains the name.
        
        # Simple fuzzy match or exact match
        # Let's try exact match on the text column
        # Finding the text column: usually the one with object dtype and not 'Code'
        name_col = None
        for col in df.columns:
            if "State" in col or "Description" in col:
                name_col = col
                break
        
        if not name_col:
             # Fallback: assume second column
             name_col = df.columns[1]

        code_col = df.columns[0] # Assume first is code

        # Case-insensitive lookup
        match = df[df[name_col].astype(str).str.lower() == name.lower()]
        if not match.empty:
            return match.iloc[0][code_col]
        else:
            raise ValueError(f"State '{name}' not found in metadata.")

    def lookup_mode(self, name):
        """Look up mode code by name."""
        df = self.modes
        name_col = None
        for col in df.columns:
            if "Mode" in col or "Description" in col:
                name_col = col
                break
        if not name_col: name_col = df.columns[1]
        code_col = df.columns[0]

        match = df[df[name_col].astype(str).str.lower() == name.lower()]
        if not match.empty:
            return match.iloc[0][code_col]
        else:
            raise ValueError(f"Mode '{name}' not found in metadata.")

    def lookup_commodity(self, name):
        """Look up commodity code by name."""
        df = self.commodities
        name_col = None
        for col in df.columns:
            if "Description" in col or "Commodity" in col:
                name_col = col
                break
        if not name_col: name_col = df.columns[1]
        code_col = df.columns[0]

        match = df[df[name_col].astype(str).str.lower() == name.lower()]
        if not match.empty:
            return match.iloc[0][code_col]
        else:
            raise ValueError(f"Commodity '{name}' not found in metadata.")

    def lookup_zone(self, name):
        """Look up FAF Zone code by name."""
        df = self.zones
        # Search in 'Description' or 'Long Description'
        # Usually 'Description' is short, 'Long Description' has full name
        # We'll search both if available
        
        cols_to_search = []
        for col in df.columns:
            if "Description" in col:
                cols_to_search.append(col)
        
        if not cols_to_search:
            # Fallback
            cols_to_search = [df.columns[1]]
            
        code_col = df.columns[0]
        
        for col in cols_to_search:
            match = df[df[col].astype(str).str.contains(name, case=False, na=False)]
            if not match.empty:
                return match.iloc[0][code_col]
                
        raise ValueError(f"Zone '{name}' not found in metadata.")
            
    def load_variables(self, dataset="all"):
        """
        Print available codes and descriptions.
        """
        print("Available States:")
        print(self.states.head())
        print("\nAvailable Modes:")
        print(self.modes.head())
        print("\nAvailable Commodities:")
        print(self.commodities.head())
        print("\nAvailable Zones:")
        print(self.zones.head())
