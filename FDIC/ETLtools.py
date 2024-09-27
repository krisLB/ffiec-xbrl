import FDIC.constants as paths
import os

class ETLtools:
    ALLOWED_FILETYPES = ['csv', 'pdf', 'txt', 'xbrl']
    SAFE_ORIGIN_FOLDERS = [paths.localPath + paths.folder_Orig]

    @staticmethod
    def remove_files(folderpath: str, filetypes: list):
        """
        Removes files in the given folderpath that match the allowed filetypes.
        
        Parameters:
        folderpath (str): The path of the folder to delete files from.
        filetypes (list): A list of file extensions to search for and delete.
        """

        # Check if folderpath is in the list of safe origin folders
        if not any(os.path.commonpath([folderpath, safe_folder]) for safe_folder in ETLtools.SAFE_ORIGIN_FOLDERS):
            raise ValueError("The folderpath is not in the list of safe origin folders.")

        # Filter allowed filetypes
        for filetype in filetypes:
            if filetype not in ETLtools.ALLOWED_FILETYPES:
                raise ValueError(f"Filetype {filetype} is not allowed.")

        # Traverse and delete files with matching filetypes
        for root, dirs, files in os.walk(folderpath):
            for file in files:
                if file.split('.')[-1] in filetypes:
                    filepath = os.path.join(root, file)
                    os.remove(filepath)
                    print(f"Deleted: {filepath}")


    



    





