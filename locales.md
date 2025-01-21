erDiagram

    LANGUAGE ||--|{ LOCALE : "verweist auf"
    LANGUAGE ||--o{ PRODUCT_TRANSLATION : "wird genutzt von"
    LANGUAGE ||--o{ CATEGORY_TRANSLATION : "wird genutzt von"

    %% Sprache verweist optional auf eine Fallback-Sprache in der selben Tabelle
    LANGUAGE }|--|| LANGUAGE : "Fallback-Verweis"

    LOCALE ||--|{ LANGUAGE : "wird referenziert von"

    TRANSLATION_SET ||--|{ TRANSLATION_STRING : "enthält"

    PRODUCT ||--o{ PRODUCT_TRANSLATION : "wird übersetzt in"
    CATEGORY ||--o{ CATEGORY_TRANSLATION : "wird übersetzt in"

    LANGUAGE {
        uuid id PK
        uuid locale_id FK
        varchar name
        uuid fallback_language_id FK "Selbstverweis auf 'language.id'"
    }

    LOCALE {
        uuid id PK
        varchar code
        varchar territory
    }

    PRODUCT {
        uuid id PK
    }

    PRODUCT_TRANSLATION {
        uuid product_id PK,FK
        uuid language_id PK,FK
        varchar name
        text description
    }

    CATEGORY {
        uuid id PK
    }

    CATEGORY_TRANSLATION {
        uuid category_id PK,FK
        uuid language_id PK,FK
        varchar name
    }

    TRANSLATION_SET {
        uuid id PK
        varchar name
        varchar base_file
    }

    TRANSLATION_STRING {
        uuid id PK
        uuid translation_set_id FK
        varchar translation_key
        text value
    }
