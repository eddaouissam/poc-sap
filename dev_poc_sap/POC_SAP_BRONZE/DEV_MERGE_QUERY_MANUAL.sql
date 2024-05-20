
-- Retrieve les clés primaire d'une table X
SELECT fieldname
    FROM SAP_BRONZE.B_dd03l
    WHERE UPPER(tabname) = UPPER('ADR6') AND keyflag = 'X' AND fieldname != '.INCLUDE';


-- Fusionner dans la table cible
MERGE INTO SAP_SILVER.S_ADR6 AS T
        USING (
            -- Sélectionner les lignes les plus récentes
            SELECT *
            FROM (
                -- Ajouter un numéro de ligne pour chaque groupe
                SELECT *, ROW_NUMBER() OVER (PARTITION BY CONSNUMBER, PERSNUMBER, ADDRNUMBER, CLIENT, DATE_FROM ORDER BY recordstamp DESC) AS rn
                FROM SAP_BRONZE.B_ADR6
            ) a
            WHERE rn = 1
        ) S ON 
        -- Conditions de jointure !!!
        (T.CONSNUMBER = S.CONSNUMBER OR (T.CONSNUMBER IS NULL AND S.CONSNUMBER IS NULL)) 
        AND (T.PERSNUMBER = S.PERSNUMBER OR (T.PERSNUMBER IS NULL AND S.PERSNUMBER IS NULL)) 
        AND (T.ADDRNUMBER = S.ADDRNUMBER OR (T.ADDRNUMBER IS NULL AND S.ADDRNUMBER IS NULL)) 
        AND (T.CLIENT = S.CLIENT OR (T.CLIENT IS NULL AND S.CLIENT IS NULL)) 
        AND (T.DATE_FROM = S.DATE_FROM OR (T.DATE_FROM IS NULL AND S.DATE_FROM IS NULL))

        -- Insérer les nouvelles lignes
        WHEN NOT MATCHED AND COALESCE(S.operation_flag, 'I') != 'D' THEN
            INSERT (R3_USER, SMTP_SRCH, VALID_TO, VALID_FROM, FLG_NOUSE, CONSNUMBER, ENCODE, TNEF, RECORDSTAMP, DATE_FROM, SMTP_ADDR, PERSNUMBER, DFT_RECEIV, HOME_FLAG, FLGDEFAULT, ADDRNUMBER, CLIENT) 
            VALUES (S.R3_USER, S.SMTP_SRCH, S.VALID_TO, S.VALID_FROM, S.FLG_NOUSE, S.CONSNUMBER, S.ENCODE, S.TNEF, S.RECORDSTAMP, S.DATE_FROM, S.SMTP_ADDR, S.PERSNUMBER, S.DFT_RECEIV, S.HOME_FLAG, S.FLGDEFAULT, S.ADDRNUMBER, S.CLIENT)

        -- Mettre à jour les lignes existantes
        WHEN MATCHED AND S.operation_flag IN ('I', 'U') THEN
            UPDATE SET T.R3_USER = S.R3_USER, T.SMTP_SRCH = S.SMTP_SRCH, T.VALID_TO = S.VALID_TO, T.VALID_FROM = S.VALID_FROM, T.FLG_NOUSE = S.FLG_NOUSE, T.CONSNUMBER = S.CONSNUMBER, T.ENCODE = S.ENCODE, T.TNEF = S.TNEF, T.RECORDSTAMP = S.RECORDSTAMP, T.DATE_FROM = S.DATE_FROM, T.SMTP_ADDR = S.SMTP_ADDR, T.PERSNUMBER = S.PERSNUMBER, T.DFT_RECEIV = S.DFT_RECEIV, T.HOME_FLAG = S.HOME_FLAG, T.FLGDEFAULT = S.FLGDEFAULT, T.ADDRNUMBER = S.ADDRNUMBER, T.CLIENT = S.CLIENT

        -- Supprimer les lignes correspondantes
        WHEN MATCHED AND S.operation_flag = 'D' THEN
            DELETE;

