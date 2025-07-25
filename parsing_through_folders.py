import os
import shutil

# Path to the root directory where PNGs are searched
source_folder = r"C:\Users\shivanandk\Desktop\Limited Edition 1000+ Premium Car T-Shirt Design Bundle"

# Path to the destination folder (it will be created if it doesn't exist)
destination_folder = r"C:\Users\shivanandk\Desktop\Govt pdf\911\viacode"
os.makedirs(destination_folder, exist_ok=True)

# ✅ 2. Make sure destination exists
os.makedirs(destination_folder, exist_ok=True)

# ✅ 3. Walk through all directories and subdirectories
# for root, dirs, files in os.walk(source_folder):
#     for file in files:
#         if file.lower().endswith(".png"):
#             source_file = os.path.join(root, file)
#             destination_file = os.path.join(destination_folder, file)

#             # ✅ Avoid filename collision
#             if os.path.exists(destination_file):
#                 base, ext = os.path.splitext(file)
#                 counter = 1
#                 while os.path.exists(destination_file):
#                     new_file = f"{base}_{counter}{ext}"
#                     destination_file = os.path.join(destination_folder, new_file)
#                     counter += 1

#             # ✅ Copy file
#             shutil.copy2(source_file, destination_file)

# print("✅ All PNG files copied successfully.")

def print_tree(start_path, prefix=""):
    entries = sorted(os.listdir(start_path))
    entries_count = len(entries)

    for i, entry in enumerate(entries):
        path = os.path.join(start_path, entry)
        connector = "└── " if i == entries_count - 1 else "├── "
        print(prefix + connector + entry)

        if os.path.isdir(path):
            extension = "    " if i == entries_count - 1 else "│   "
            print_tree(path, prefix + extension)
    
    
# Ensure destination exists
os.makedirs(destination_folder, exist_ok=True)

# Traverse through the entire directory tree
for root, dirs, files in os.walk(source_folder):
    for file in files:
        if file.lower().endswith((".png", ".jpg")):
            # Full original path
            source_file = os.path.join(root, file)

            # Build relative path from source_folder
            relative_path = os.path.relpath(root, source_folder)

            # Replace os separators (/) with underscores (_) and build new filename
            prefix = relative_path.replace(os.sep, "_")
            if prefix != ".":
                new_filename = f"{prefix}_{file}"
            else:
                new_filename = file

            # Full destination path
            destination_file = os.path.join(destination_folder, new_filename)

            # Avoid filename collision
            counter = 1
            base_name, ext = os.path.splitext(new_filename)
            while os.path.exists(destination_file):
                new_filename = f"{base_name}_{counter}{ext}"
                destination_file = os.path.join(destination_folder, new_filename)
                counter += 1

            # Copy the file
            shutil.copy2(source_file, destination_file)

print(os.path.basename(source_folder) + "/")
print_tree(source_folder)