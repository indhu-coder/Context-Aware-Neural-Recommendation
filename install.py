# import os
# import kagglehub

# # 1. Log in with your Kaggle credentials
# kagglehub.login()

# # 2. Define the path inside your actual D drive project environment folder
# my_project_path = r"D:\\Context Aware Recommendation system\\Context-Aware-Neural-Recommendation\\recomm_env\\data"

# # Create the directory if it doesn't exist yet
# os.makedirs(my_project_path, exist_ok=True)

# # 3. Download AND unzip directly inside your D drive path 
# print("Downloading and extracting directly to D drive... This will take a while.")
# path = kagglehub.competition_download(
#     'h-and-m-personalized-fashion-recommendations',
#     output_dir=my_project_path
# )
# print("\n🎉 SUCCESS! Competition files are safely extracted at:", path)
