library(dplyr)
library(ggplot2)
library(sf)
library(rnaturalearth)
library(countrycode)

# download this: https://docs.google.com/spreadsheets/d/1wxSa1y6qQ95bCLnKEyz9gKfF-K2pI_qoT7qbn4JS-kY/edit?gid=532384564#gid=532384564
# read database
df <- read.csv("demographics.csv", comment.char = "#")

# Drop rows that aren't countries (like "(not set)")
df <- df %>% filter(Country != "(not set)")

# --- 2) Normalize country names -> ISO3 for a reliable join ---
fixups <- c(
  "United States"         = "United States of America",
  "South Korea"           = "Korea, Republic of",
  "North Korea"           = "Korea, Democratic People's Republic of",
  "Russia"                = "Russian Federation",
  "Türkiye"               = "Turkey",
  "Côte d’Ivoire"         = "Cote d'Ivoire",
  "Congo - Kinshasa"      = "Congo, The Democratic Republic of the",
  "Congo (Kinshasa)"      = "Congo, The Democratic Republic of the",
  "Congo (Brazzaville)"   = "Congo",
  "Myanmar (Burma)"       = "Myanmar",
  "Hong Kong"             = "Hong Kong SAR China",
  "Macao"                 = "Macao SAR China",
  "Palestine"             = "Palestine, State of",
  "Micronesia"            = "Micronesia, Federated States of",
  "Eswatini"              = "Swaziland"
)

df_iso <- df %>%
  mutate(
    Country_std = dplyr::recode(Country, !!!fixups, .default = Country),
    iso3 = countrycode(Country_std, origin = "country.name", destination = "iso3c")
  ) %>%
  group_by(iso3) %>%
  summarize(active_users = sum(as.numeric(Active.users), na.rm = TRUE), .groups = "drop")

# --- 3) Get world geometry and join ---
world <- ne_countries(scale = "medium", returnclass = "sf") %>%
  st_make_valid()

world_joined <- world %>%
  left_join(df_iso, by = c("iso_a3" = "iso3"))

# --- 4) Plot (log scale helps with the long tail) ---
ggplot(world_joined) +
  geom_sf(aes(fill = active_users), color = "white", size = 0.1) +
  # scale_fill_viridis_c(
  #   trans = "log10",
  #   na.value = "grey90",
  #   labels = scales::label_number_si(),
  #   name = "Active users"
  # ) +
  scale_fill_gradient(
    low = "light blue", high = "dark blue", na.value = "grey90", trans = "log10", labels = scales::label_number_si(), name = "Active users"
  ) +
  # labs(title = "Active users by country") +
  theme_void() +
  theme(
    legend.position = "right",
    plot.title = element_text(hjust = 0.5, face = "bold")
  )
