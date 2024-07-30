#!/bin/bash

# Definir schema de la source et le stage
source_path="/////"
stage_path="@SAP_CSV_FILES"

# Boucle pour chaque fichier present
for filepath in "$source_path"/*; do
    # Modifier le nom du dossier
    filename=$(basename "$filepath" | cut -f 1 -d '.')

    # Construct the target path in the stage
    target_path="$stage_path/$filename/"

    # Executer la commande PUT afin de charger les fichiers (chcun dans un dossier convenable)dans Snowflake 
    echo "PUT file://$filepath $target_path;"
    snowsql -q "PUT file://$filepath $target_path;"
done
