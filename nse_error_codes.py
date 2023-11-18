import pandas as pd
from functools import lru_cache
from typing import Literal, Union


@lru_cache(maxsize=20)
def get_codes(retry: int=1) -> pd.DataFrame:
    try:
        errcodes = pd.read_csv("https://raw.githubusercontent.com/Tapanhaz/Nse_Error_Codes/main/Error_Codes.csv")
        return errcodes
    except Exception as e:
        print(f"Error occured :: {e}")
        get_codes.cache_clear()
        if retry <= 3:
            print(f"Retrying :: {retry}")
            return get_codes(retry= retry+1)
        else:
            return pd.DataFrame()

def check_error(
        segment: Literal["CM", "FO", "CD"]="FO",
        error_code: Union[str, int]=None,
        error_id: str= None
        ) -> dict:
    query_parts = []
    df = get_codes()
    if not df.empty:
        query_parts.append(f"Segment in ['{segment}']"  )

        if error_code:
            query_parts.append(f"Error_Code in [{int(error_code)}]")
        if error_id:
            query_parts.append(f"Error_ID in ['{error_id}']")

        query = ' and '.join(query_parts)
        result = df.query(query)

        if not result.empty:
            result = result.iloc[[0]]
            result.columns = ["segment", "error_code", "error_id", "description"]
            output = result.to_dict(orient='records')[0]        
            return output

if __name__ == "__main__":
    # Default segment is "FO"
    err = check_error(error_code=16285)
    print(err)
    err = check_error(segment="CM", error_code= 16387)
    print(err)
    err = check_error(segment="CD", error_id="OE_AUCTION_PENDING")
    print(err)
