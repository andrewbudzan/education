create or replace database view_db;

create secure view supp_info as
    select s.supp_id, s.supp_name, s2.supp_address
    from party.public.supplier s
        inner join contact.public.supp_address s2
            on s.supp_id = s2.supp_id;
