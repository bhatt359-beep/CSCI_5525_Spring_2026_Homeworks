from gdown import download
import re

links = ['https://drive.google.com/file/d/1IssAtSKMpY9bA3ZzSBB7AUtQtufSibnz/view?usp=drive_link',
         'https://drive.google.com/file/d/1VBCCD48TupHCXHhMB34jgBS4c9y68Jqo/view?usp=drive_link',
         'https://drive.google.com/file/d/174sER2LTguTj_q4o4jyK8Os-FjV-UUof/view?usp=drive_link',
         'https://drive.google.com/file/d/1hIt90iJD3wA9dkaiMWM__RMdI9fpPZ76/view?usp=drive_link']

for link in links:
    link_id = re.search(r'd/.+/v', link).group()
    link_id = link_id[2:len(link_id)-2]
    
    clean_link = f'https://drive.google.com/uc?id={link_id}'
    
    download(clean_link)
