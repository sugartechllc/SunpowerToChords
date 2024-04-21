# SunpowerToChords

Send an Excel spreadsheet to CHORDS. 

- One column must be named *Period*. It contains the timestamp for each row of data,
  and the spreadsheet datatype for this column must be text.
- The other column names can be whatever you want.
- The timestamp can be either an ISO formated string, or the time period in 
  Sunpower Datetime format (Saturday, 2/12/2022 - 7:00am - 8:00am for example).
- The .json configuration matches the column names to CHORDS short names.

**TODO:** The timezone is hardwired to *US/Pacific*. Add an optional command line
switch to allow this to be changed.
