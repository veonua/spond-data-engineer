from pyspark.sql import SparkSession
from pyspark.sql.functions import length

root = r"/datalake/"
test = False

def load(spark: SparkSession):
    profiles = spark.read.option("multiline", "true").json(root+r"profile/created_at=2023-01-05/profile.json")
    profiles = profiles.filter(profiles.externalid.isNotNull())

    unsubscribe = spark.read.text(root+r"unsubscribe/unsubscribe.csv")

    # filter non email lines shorter than 5 characters
    unsubscribe = unsubscribe.filter(length(unsubscribe.value) > 5)
    # how many do not have an email address
    assert unsubscribe.filter(~unsubscribe.value.contains("@")).count() == 0

    group_ds = spark.read.parquet(root+r"group/group.parquet")
    group_ds = group_ds.filter(group_ds.profile_id.isNotNull()) # select profiles with non-null profile_id from selected club admins

    # Test that dataframes are not empty
    assert profiles.count() > 0
    assert unsubscribe.count() > 0
    assert group_ds.count() > 0

    return profiles, unsubscribe, group_ds


def run(profiles, unsubscribe, group_ds):
    # Create an email list of admins from the USA from Running Club & Tennis club.please exclude any emails who have unsubscribed.
    selected_club_admins = group_ds[(group_ds['is_admin'] == True) & (group_ds['group_name'].isin(['Running Club', 'Tennis Club']))]

    # only admin profiles of selected clubs
    selected_profiles = profiles.join(selected_club_admins, profiles.profile_id == selected_club_admins.profile_id, 'inner')
    # not in unsubscribe list 
    valid_profiles = selected_profiles.join(unsubscribe, selected_profiles.email == unsubscribe.value, 'left_anti') 

    assert valid_profiles[valid_profiles.is_admin == False].count() == 0     # assert is_admin is true for all selected profiles
    assert valid_profiles[valid_profiles.externalid.isNull()].count() == 0  # assert external_id is not null for all selected profiles
    assert {r.group_name for r in valid_profiles.select("group_name").distinct().collect()} == {'Running Club', 'Tennis Club'} # assert group_name is "Running Club" or "Tennis Club"

    ## Test that unsubscribe list is correct
    if test:
        not_valid_profiles = selected_profiles.join(unsubscribe, selected_profiles.email == unsubscribe.value, 'inner')
        assert not_valid_profiles.count() == 2 and not_valid_profiles[not_valid_profiles.email.isin(['khallgarth2v@amazon.co.uk', 'awalisiakhp@yellowbook.com'])].count() == 2
    ##
    return valid_profiles.select("email")
    

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    
    profiles, unsubscribe, group_ds = load(spark)
    valid_profiles = run(profiles, unsubscribe, group_ds)
    valid_profiles.write.csv(root+r"output1.csv", header=True, mode="overwrite")