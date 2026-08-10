# Context-Aware-Neural-Recommendation

**Project Information**

A Deep Learning recommendation system for an e-commerce or content streaming platform.
Moving beyond basic collaborative filtering, this two-tower neural network leverages user metadata, historical interaction sequences, and item context to generate hyper personalized, real-time recommendations.

**Data Source**

● Primary Source: H&M Personalized Fashion Recommendations (via Kaggle).

● Characteristics: Massive dataset including user purchase histories, detailed article metadata (color, garment type), and user demographics over a multi-year period.

Downloading the dataset through kaggle API and reading the dataset through pyspark.
    
    #import os
    # import kagglehub
    
    # # 1. Log in with your Kaggle credentials
    # kagglehub.login()
    
    # # 2. Define the path inside your actual D drive project environment folder
    # my_project_path = r"D:\\Context Aware Recommendation system\\Context-Aware-Neural-Recommendation\\recomm_env\\data"
    
    # # Create the directory if it doesn't exist yet
    # os.makedirs(my_project_path, exist_ok=True)
    
    # # 3. Download AND unzip directly inside your D drive path 
    # print("Downloading and extracting directly to D drive... This will take a while.")
    # path = kagglehub.competition_download('h-and-m-personalized-fashion-recommendations',output_dir=my_project_path)
    # print("\n🎉 SUCCESS! Competition files are safely extracted at:", path)

