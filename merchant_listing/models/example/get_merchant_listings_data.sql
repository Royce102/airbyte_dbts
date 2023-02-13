{{ config (
    materialized="table"
)}}

create  table "trevcoDataWarehouse".airbyte."get_merchant_listings_data__dbt_tmp"
as (
with __dbt__cte__get_merchant_listings_data_ab1 as (

-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: "trevcoDataWarehouse".airbyte._airbyte_raw_get_merchant_listings_data
select
    jsonb_extract_path_text(_airbyte_data, 'asin1') as asin1,
    jsonb_extract_path_text(_airbyte_data, 'asin2') as asin2,
    jsonb_extract_path_text(_airbyte_data, 'asin3') as asin3,
    jsonb_extract_path_text(_airbyte_data, 'price') as price,
    jsonb_extract_path_text(_airbyte_data, 'quantity') as quantity,
    jsonb_extract_path_text(_airbyte_data, 'image-url') as "image-url",
    jsonb_extract_path_text(_airbyte_data, 'item-name') as "item-name",
    jsonb_extract_path_text(_airbyte_data, 'item-note') as "item-note",
    jsonb_extract_path_text(_airbyte_data, 'open-date') as "open-date",
    jsonb_extract_path_text(_airbyte_data, 'add-delete') as "add-delete",
    jsonb_extract_path_text(_airbyte_data, 'listing-id') as "listing-id",
    jsonb_extract_path_text(_airbyte_data, 'product-id') as "product-id",
    jsonb_extract_path_text(_airbyte_data, 'seller-sku') as "seller-sku",
    jsonb_extract_path_text(_airbyte_data, 'Business Price') as "Business Price",
    jsonb_extract_path_text(_airbyte_data, 'item-condition') as "item-condition",
    jsonb_extract_path_text(_airbyte_data, 'zshop-boldface') as "zshop-boldface",
    jsonb_extract_path_text(_airbyte_data, 'product-id-type') as "product-id-type",
    jsonb_extract_path_text(_airbyte_data, 'zshop-category1') as "zshop-category1",
    jsonb_extract_path_text(_airbyte_data, 'Quantity Price 1') as "Quantity Price 1",
    jsonb_extract_path_text(_airbyte_data, 'Quantity Price 2') as "Quantity Price 2",
    jsonb_extract_path_text(_airbyte_data, 'Quantity Price 3') as "Quantity Price 3",
    jsonb_extract_path_text(_airbyte_data, 'Quantity Price 4') as "Quantity Price 4",
    jsonb_extract_path_text(_airbyte_data, 'Quantity Price 5') as "Quantity Price 5",
    jsonb_extract_path_text(_airbyte_data, 'item-description') as "item-description",
    jsonb_extract_path_text(_airbyte_data, 'pending-quantity') as "pending-quantity",
    jsonb_extract_path_text(_airbyte_data, 'zshop-browse-path') as "zshop-browse-path",
    jsonb_extract_path_text(_airbyte_data, 'expedited-shipping') as "expedited-shipping",
    jsonb_extract_path_text(_airbyte_data, 'zshop-shipping-fee') as "zshop-shipping-fee",
    jsonb_extract_path_text(_airbyte_data, 'Progressive Price 1') as "Progressive Price 1",
    jsonb_extract_path_text(_airbyte_data, 'Progressive Price 2') as "Progressive Price 2",
    jsonb_extract_path_text(_airbyte_data, 'Progressive Price 3') as "Progressive Price 3",
    jsonb_extract_path_text(_airbyte_data, 'Quantity Price Type') as "Quantity Price Type",
    jsonb_extract_path_text(_airbyte_data, 'fulfillment-channel') as "fulfillment-channel",
    jsonb_extract_path_text(_airbyte_data, 'item-is-marketplace') as "item-is-marketplace",
    jsonb_extract_path_text(_airbyte_data, 'Progressive Price Type') as "Progressive Price Type",
    jsonb_extract_path_text(_airbyte_data, 'Quantity Lower Bound 1') as "Quantity Lower Bound 1",
    jsonb_extract_path_text(_airbyte_data, 'Quantity Lower Bound 2') as "Quantity Lower Bound 2",
    jsonb_extract_path_text(_airbyte_data, 'Quantity Lower Bound 3') as "Quantity Lower Bound 3",
    jsonb_extract_path_text(_airbyte_data, 'Quantity Lower Bound 4') as "Quantity Lower Bound 4",
    jsonb_extract_path_text(_airbyte_data, 'Quantity Lower Bound 5') as "Quantity Lower Bound 5",
    jsonb_extract_path_text(_airbyte_data, 'merchant-shipping-group') as "merchant-shipping-group",
    jsonb_extract_path_text(_airbyte_data, 'zshop-storefront-feature') as "zshop-storefront-feature",
    jsonb_extract_path_text(_airbyte_data, 'Progressive Lower Bound 1') as "Progressive Lower Bound 1",
    jsonb_extract_path_text(_airbyte_data, 'Progressive Lower Bound 2') as "Progressive Lower Bound 2",
    jsonb_extract_path_text(_airbyte_data, 'Progressive Lower Bound 3') as "Progressive Lower Bound 3",
    jsonb_extract_path_text(_airbyte_data, 'will-ship-internationally') as "will-ship-internationally",
    jsonb_extract_path_text(_airbyte_data, 'bid-for-featured-placement') as "bid-for-featured-placement",
    'APOK1IN7I76R' as "merchant_id",
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from "trevcoDataWarehouse".airbyte._airbyte_raw_get_merchant_listings_data as table_alias
-- get_merchant_listings_data
where 1 = 1
),  __dbt__cte__get_merchant_listings_data_ab2 as (

-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: __dbt__cte__get_merchant_listings_data_ab1
select
    cast(asin1 as text) as asin1,
    cast(asin2 as text) as asin2,
    cast(asin3 as text) as asin3,
    cast(price as text) as price,
    cast(quantity as text) as quantity,
    cast("image-url" as text) as "image-url",
    cast("item-name" as text) as "item-name",
    cast("item-note" as text) as "item-note",
    cast("open-date" as text) as "open-date",
    cast("add-delete" as text) as "add-delete",
    cast("listing-id" as text) as "listing-id",
    cast("product-id" as text) as "product-id",
    cast("seller-sku" as text) as "seller-sku",
    cast("Business Price" as text) as "Business Price",
    cast("item-condition" as text) as "item-condition",
    cast("zshop-boldface" as text) as "zshop-boldface",
    cast("product-id-type" as text) as "product-id-type",
    cast("zshop-category1" as text) as "zshop-category1",
    cast("Quantity Price 1" as text) as "Quantity Price 1",
    cast("Quantity Price 2" as text) as "Quantity Price 2",
    cast("Quantity Price 3" as text) as "Quantity Price 3",
    cast("Quantity Price 4" as text) as "Quantity Price 4",
    cast("Quantity Price 5" as text) as "Quantity Price 5",
    cast("item-description" as text) as "item-description",
    cast("pending-quantity" as text) as "pending-quantity",
    cast("zshop-browse-path" as text) as "zshop-browse-path",
    cast("expedited-shipping" as text) as "expedited-shipping",
    cast("zshop-shipping-fee" as text) as "zshop-shipping-fee",
    cast("Progressive Price 1" as text) as "Progressive Price 1",
    cast("Progressive Price 2" as text) as "Progressive Price 2",
    cast("Progressive Price 3" as text) as "Progressive Price 3",
    cast("Quantity Price Type" as text) as "Quantity Price Type",
    cast("fulfillment-channel" as text) as "fulfillment-channel",
    cast("item-is-marketplace" as text) as "item-is-marketplace",
    cast("Progressive Price Type" as text) as "Progressive Price Type",
    cast("Quantity Lower Bound 1" as text) as "Quantity Lower Bound 1",
    cast("Quantity Lower Bound 2" as text) as "Quantity Lower Bound 2",
    cast("Quantity Lower Bound 3" as text) as "Quantity Lower Bound 3",
    cast("Quantity Lower Bound 4" as text) as "Quantity Lower Bound 4",
    cast("Quantity Lower Bound 5" as text) as "Quantity Lower Bound 5",
    cast("merchant-shipping-group" as text) as "merchant-shipping-group",
    cast("zshop-storefront-feature" as text) as "zshop-storefront-feature",
    cast("Progressive Lower Bound 1" as text) as "Progressive Lower Bound 1",
    cast("Progressive Lower Bound 2" as text) as "Progressive Lower Bound 2",
    cast("Progressive Lower Bound 3" as text) as "Progressive Lower Bound 3",
    cast("will-ship-internationally" as text) as "will-ship-internationally",
    cast("bid-for-featured-placement" as text) as "bid-for-featured-placement",
    "merchant_id",
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at
from __dbt__cte__get_merchant_listings_data_ab1
-- get_merchant_listings_data
where 1 = 1
),  __dbt__cte__get_merchant_listings_data_ab3 as (

-- SQL model to build a hash column based on the values of this record
-- depends_on: __dbt__cte__get_merchant_listings_data_ab2
select
    md5(cast(coalesce(cast(asin1 as text), '') || '-' || coalesce(cast(asin2 as text), '') || '-' || coalesce(cast(asin3 as text), '') || '-' || coalesce(cast(price as text), '') || '-' || coalesce(cast(quantity as text), '') || '-' || coalesce(cast("image-url" as text), '') || '-' || coalesce(cast("item-name" as text), '') || '-' || coalesce(cast("item-note" as text), '') || '-' || coalesce(cast("open-date" as text), '') || '-' || coalesce(cast("add-delete" as text), '') || '-' || coalesce(cast("listing-id" as text), '') || '-' || coalesce(cast("product-id" as text), '') || '-' || coalesce(cast("seller-sku" as text), '') || '-' || coalesce(cast("Business Price" as text), '') || '-' || coalesce(cast("item-condition" as text), '') || '-' || coalesce(cast("zshop-boldface" as text), '') || '-' || coalesce(cast("product-id-type" as text), '') || '-' || coalesce(cast("zshop-category1" as text), '') || '-' || coalesce(cast("Quantity Price 1" as text), '') || '-' || coalesce(cast("Quantity Price 2" as text), '') || '-' || coalesce(cast("Quantity Price 3" as text), '') || '-' || coalesce(cast("Quantity Price 4" as text), '') || '-' || coalesce(cast("Quantity Price 5" as text), '') || '-' || coalesce(cast("item-description" as text), '') || '-' || coalesce(cast("pending-quantity" as text), '') || '-' || coalesce(cast("zshop-browse-path" as text), '') || '-' || coalesce(cast("expedited-shipping" as text), '') || '-' || coalesce(cast("zshop-shipping-fee" as text), '') || '-' || coalesce(cast("Progressive Price 1" as text), '') || '-' || coalesce(cast("Progressive Price 2" as text), '') || '-' || coalesce(cast("Progressive Price 3" as text), '') || '-' || coalesce(cast("Quantity Price Type" as text), '') || '-' || coalesce(cast("fulfillment-channel" as text), '') || '-' || coalesce(cast("item-is-marketplace" as text), '') || '-' || coalesce(cast("Progressive Price Type" as text), '') || '-' || coalesce(cast("Quantity Lower Bound 1" as text), '') || '-' || coalesce(cast("Quantity Lower Bound 2" as text), '') || '-' || coalesce(cast("Quantity Lower Bound 3" as text), '') || '-' || coalesce(cast("Quantity Lower Bound 4" as text), '') || '-' || coalesce(cast("Quantity Lower Bound 5" as text), '') || '-' || coalesce(cast("merchant-shipping-group" as text), '') || '-' || coalesce(cast("zshop-storefront-feature" as text), '') || '-' || coalesce(cast("Progressive Lower Bound 1" as text), '') || '-' || coalesce(cast("Progressive Lower Bound 2" as text), '') || '-' || coalesce(cast("Progressive Lower Bound 3" as text), '') || '-' || coalesce(cast("will-ship-internationally" as text), '') || '-' || coalesce(cast("bid-for-featured-placement" as text), '') as text)
    || '-' || coalesce(cast(merchant_id as text), '') 
    ) 
    as _airbyte_get_merchant_listings_data_hashid,
    tmp.*
from __dbt__cte__get_merchant_listings_data_ab2 tmp
-- get_merchant_listings_data
where 1 = 1
)-- Final base SQL model
-- depends_on: __dbt__cte__get_merchant_listings_data_ab3
select
    asin1,
    asin2,
    asin3,
    price,
    quantity,
    "image-url",
    "item-name",
    "item-note",
    "open-date",
    "add-delete",
    "listing-id",
    "product-id",
    "seller-sku",
    "Business Price",
    "item-condition",
    "zshop-boldface",
    "product-id-type",
    "zshop-category1",
    "Quantity Price 1",
    "Quantity Price 2",
    "Quantity Price 3",
    "Quantity Price 4",
    "Quantity Price 5",
    "item-description",
    "pending-quantity",
    "zshop-browse-path",
    "expedited-shipping",
    "zshop-shipping-fee",
    "Progressive Price 1",
    "Progressive Price 2",
    "Progressive Price 3",
    "Quantity Price Type",
    "fulfillment-channel",
    "item-is-marketplace",
    "Progressive Price Type",
    "Quantity Lower Bound 1",
    "Quantity Lower Bound 2",
    "Quantity Lower Bound 3",
    "Quantity Lower Bound 4",
    "Quantity Lower Bound 5",
    "merchant-shipping-group",
    "zshop-storefront-feature",
    "Progressive Lower Bound 1",
    "Progressive Lower Bound 2",
    "Progressive Lower Bound 3",
    "will-ship-internationally",
    "bid-for-featured-placement",
    "merchant_id", 
    _airbyte_ab_id,
    _airbyte_emitted_at,
    now() as _airbyte_normalized_at,
    _airbyte_get_merchant_listings_data_hashid
from __dbt__cte__get_merchant_listings_data_ab3
-- get_merchant_listings_data from "trevcoDataWarehouse".airbyte._airbyte_raw_get_merchant_listings_data
where 1 = 1
);
