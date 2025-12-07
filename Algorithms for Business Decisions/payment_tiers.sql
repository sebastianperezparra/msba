with payments as (
    select * from {{ ref('stg_payments') }}
),

final as (
    select
        ID as payment_id,
        ORDERID as order_id,
        AMOUNT as amount,
        PAYMENTMETHOD as payment_method,
        CREATED as payment_date,

 -        -- Tier logic based on amount
        case
            when AMOUNT < 50 then 'low'
            when AMOUNT < 200 then 'medium'
            else 'high'
        end as payment_tier,

        -- Weekend flag
        case
            when extract(dow from CREATED) in (0, 6) then true
            else false
        end as is_weekend,

        -- Month extracted
        extract(month from CREATED) as payment_month
    from payments
)

select * from final