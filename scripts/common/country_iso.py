"""Country name → ISO 3166-1 alpha-2 lookup. Used by import_doc_unico.process_client_info
to populate guests.country_code from the full country names in the Excel.

Add new entries here when you encounter unmatched countries — the importer logs
them on first run.
"""
from __future__ import annotations
from typing import Optional
COUNTRY_ISO = {
    # Top by volume (Portugal Active customer base)
    'Portugal': 'PT', 'Spain': 'ES', 'United Kingdom': 'GB', 'France': 'FR',
    'Germany': 'DE', 'United States': 'US', 'USA': 'US', 'Netherlands': 'NL',
    'Switzerland': 'CH', 'Brazil': 'BR', 'Brasil': 'BR', 'Austria': 'AT',
    'Canada': 'CA', 'Belgium': 'BE', 'Ireland': 'IE', 'Poland': 'PL',
    'Israel': 'IL', 'Italy': 'IT', 'Italia': 'IT', 'Australia': 'AU',
    'South Africa': 'ZA', 'Ukraine': 'UA', 'Czech Republic': 'CZ', 'Czechia': 'CZ',
    'Finland': 'FI', 'Norway': 'NO', 'Sweden': 'SE', 'Luxembourg': 'LU',
    'Andorra': 'AD', 'Russia': 'RU', 'Latvia': 'LV', 'Hungary': 'HU',
    'Zimbabwe': 'ZW', 'Mozambique': 'MZ', 'Denmark': 'DK', 'Greece': 'GR',
    'Romania': 'RO', 'Bulgaria': 'BG', 'Croatia': 'HR', 'Serbia': 'RS',
    'Slovenia': 'SI', 'Slovakia': 'SK', 'Estonia': 'EE', 'Lithuania': 'LT',
    # Americas
    'Argentina': 'AR', 'Chile': 'CL', 'Mexico': 'MX', 'Uruguay': 'UY',
    'Colombia': 'CO', 'Peru': 'PE', 'Venezuela': 'VE',
    # Asia / Pacific
    'China': 'CN', 'Japan': 'JP', 'Korea': 'KR', 'South Korea': 'KR',
    'India': 'IN', 'Singapore': 'SG', 'Hong Kong': 'HK', 'Taiwan': 'TW',
    'Thailand': 'TH', 'Indonesia': 'ID', 'Malaysia': 'MY', 'Philippines': 'PH',
    'Vietnam': 'VN', 'New Zealand': 'NZ',
    # MENA / Africa
    'Iceland': 'IS', 'Malta': 'MT', 'Cyprus': 'CY', 'Turkey': 'TR',
    'Saudi Arabia': 'SA', 'Arábia Saudita': 'SA',
    'United Arab Emirates': 'AE', 'UAE': 'AE',
    'Qatar': 'QA', 'Kuwait': 'KW', 'Lebanon': 'LB', 'Egypt': 'EG',
    'Morocco': 'MA', 'Tunisia': 'TN', 'Algeria': 'DZ', 'Senegal': 'SN',
    'Nigeria': 'NG', 'Kenya': 'KE', 'Angola': 'AO', 'Cape Verde': 'CV',
    'Cabo Verde': 'CV',
    # Common Portuguese variants
    'Alemanha': 'DE', 'França': 'FR', 'Inglaterra': 'GB',
    'Reino Unido': 'GB', 'Estados Unidos': 'US', 'Holanda': 'NL',
    'Suíça': 'CH', 'Bélgica': 'BE', 'Polónia': 'PL', 'Irlanda': 'IE',
    'Roménia': 'RO',
    # Long tail in our Doc Único Excel
    'New Caledonia': 'NC', 'Réunion': 'RE', 'Seychelles': 'SC',
}


def to_iso(country_full: Optional[str]) -> Optional[str]:
    """Map a country full-name (e.g. 'Germany') to ISO alpha-2 ('DE'). Returns
    None if unknown. Add unmatched entries to COUNTRY_ISO.
    """
    if not country_full:
        return None
    s = country_full.strip()
    return COUNTRY_ISO.get(s)
