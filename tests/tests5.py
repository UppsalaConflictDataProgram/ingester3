from ingester3.extensions import ViewsMonth

assert ViewsMonth(501) == ViewsMonth(501)
assert ViewsMonth(509) > ViewsMonth(501)
assert ViewsMonth(529) < ViewsMonth(591)
assert ViewsMonth(591) <= ViewsMonth(591)
assert ViewsMonth(599) >= ViewsMonth(591)
