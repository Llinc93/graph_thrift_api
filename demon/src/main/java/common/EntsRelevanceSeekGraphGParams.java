/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package common;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)", date = "2020-02-11")
public class EntsRelevanceSeekGraphGParams implements org.apache.thrift.TBase<EntsRelevanceSeekGraphGParams, EntsRelevanceSeekGraphGParams._Fields>, java.io.Serializable, Cloneable, Comparable<EntsRelevanceSeekGraphGParams> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("EntsRelevanceSeekGraphGParams");

  private static final org.apache.thrift.protocol.TField ENT_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("entName", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField ATT_IDS_FIELD_DESC = new org.apache.thrift.protocol.TField("attIds", org.apache.thrift.protocol.TType.STRING, (short)2);
  private static final org.apache.thrift.protocol.TField LEVEL_FIELD_DESC = new org.apache.thrift.protocol.TField("level", org.apache.thrift.protocol.TType.STRING, (short)3);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new EntsRelevanceSeekGraphGParamsStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new EntsRelevanceSeekGraphGParamsTupleSchemeFactory();

  public @org.apache.thrift.annotation.Nullable java.lang.String entName; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String attIds; // required
  public @org.apache.thrift.annotation.Nullable java.lang.String level; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ENT_NAME((short)1, "entName"),
    ATT_IDS((short)2, "attIds"),
    LEVEL((short)3, "level");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // ENT_NAME
          return ENT_NAME;
        case 2: // ATT_IDS
          return ATT_IDS;
        case 3: // LEVEL
          return LEVEL;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ENT_NAME, new org.apache.thrift.meta_data.FieldMetaData("entName", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.ATT_IDS, new org.apache.thrift.meta_data.FieldMetaData("attIds", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.LEVEL, new org.apache.thrift.meta_data.FieldMetaData("level", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(EntsRelevanceSeekGraphGParams.class, metaDataMap);
  }

  public EntsRelevanceSeekGraphGParams() {
  }

  public EntsRelevanceSeekGraphGParams(
    java.lang.String entName,
    java.lang.String attIds,
    java.lang.String level)
  {
    this();
    this.entName = entName;
    this.attIds = attIds;
    this.level = level;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public EntsRelevanceSeekGraphGParams(EntsRelevanceSeekGraphGParams other) {
    if (other.isSetEntName()) {
      this.entName = other.entName;
    }
    if (other.isSetAttIds()) {
      this.attIds = other.attIds;
    }
    if (other.isSetLevel()) {
      this.level = other.level;
    }
  }

  public EntsRelevanceSeekGraphGParams deepCopy() {
    return new EntsRelevanceSeekGraphGParams(this);
  }

  @Override
  public void clear() {
    this.entName = null;
    this.attIds = null;
    this.level = null;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getEntName() {
    return this.entName;
  }

  public EntsRelevanceSeekGraphGParams setEntName(@org.apache.thrift.annotation.Nullable java.lang.String entName) {
    this.entName = entName;
    return this;
  }

  public void unsetEntName() {
    this.entName = null;
  }

  /** Returns true if field entName is set (has been assigned a value) and false otherwise */
  public boolean isSetEntName() {
    return this.entName != null;
  }

  public void setEntNameIsSet(boolean value) {
    if (!value) {
      this.entName = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getAttIds() {
    return this.attIds;
  }

  public EntsRelevanceSeekGraphGParams setAttIds(@org.apache.thrift.annotation.Nullable java.lang.String attIds) {
    this.attIds = attIds;
    return this;
  }

  public void unsetAttIds() {
    this.attIds = null;
  }

  /** Returns true if field attIds is set (has been assigned a value) and false otherwise */
  public boolean isSetAttIds() {
    return this.attIds != null;
  }

  public void setAttIdsIsSet(boolean value) {
    if (!value) {
      this.attIds = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getLevel() {
    return this.level;
  }

  public EntsRelevanceSeekGraphGParams setLevel(@org.apache.thrift.annotation.Nullable java.lang.String level) {
    this.level = level;
    return this;
  }

  public void unsetLevel() {
    this.level = null;
  }

  /** Returns true if field level is set (has been assigned a value) and false otherwise */
  public boolean isSetLevel() {
    return this.level != null;
  }

  public void setLevelIsSet(boolean value) {
    if (!value) {
      this.level = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case ENT_NAME:
      if (value == null) {
        unsetEntName();
      } else {
        setEntName((java.lang.String)value);
      }
      break;

    case ATT_IDS:
      if (value == null) {
        unsetAttIds();
      } else {
        setAttIds((java.lang.String)value);
      }
      break;

    case LEVEL:
      if (value == null) {
        unsetLevel();
      } else {
        setLevel((java.lang.String)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case ENT_NAME:
      return getEntName();

    case ATT_IDS:
      return getAttIds();

    case LEVEL:
      return getLevel();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case ENT_NAME:
      return isSetEntName();
    case ATT_IDS:
      return isSetAttIds();
    case LEVEL:
      return isSetLevel();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof EntsRelevanceSeekGraphGParams)
      return this.equals((EntsRelevanceSeekGraphGParams)that);
    return false;
  }

  public boolean equals(EntsRelevanceSeekGraphGParams that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_entName = true && this.isSetEntName();
    boolean that_present_entName = true && that.isSetEntName();
    if (this_present_entName || that_present_entName) {
      if (!(this_present_entName && that_present_entName))
        return false;
      if (!this.entName.equals(that.entName))
        return false;
    }

    boolean this_present_attIds = true && this.isSetAttIds();
    boolean that_present_attIds = true && that.isSetAttIds();
    if (this_present_attIds || that_present_attIds) {
      if (!(this_present_attIds && that_present_attIds))
        return false;
      if (!this.attIds.equals(that.attIds))
        return false;
    }

    boolean this_present_level = true && this.isSetLevel();
    boolean that_present_level = true && that.isSetLevel();
    if (this_present_level || that_present_level) {
      if (!(this_present_level && that_present_level))
        return false;
      if (!this.level.equals(that.level))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetEntName()) ? 131071 : 524287);
    if (isSetEntName())
      hashCode = hashCode * 8191 + entName.hashCode();

    hashCode = hashCode * 8191 + ((isSetAttIds()) ? 131071 : 524287);
    if (isSetAttIds())
      hashCode = hashCode * 8191 + attIds.hashCode();

    hashCode = hashCode * 8191 + ((isSetLevel()) ? 131071 : 524287);
    if (isSetLevel())
      hashCode = hashCode * 8191 + level.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(EntsRelevanceSeekGraphGParams other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetEntName()).compareTo(other.isSetEntName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEntName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.entName, other.entName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetAttIds()).compareTo(other.isSetAttIds());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAttIds()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.attIds, other.attIds);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetLevel()).compareTo(other.isSetLevel());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLevel()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.level, other.level);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("EntsRelevanceSeekGraphGParams(");
    boolean first = true;

    sb.append("entName:");
    if (this.entName == null) {
      sb.append("null");
    } else {
      sb.append(this.entName);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("attIds:");
    if (this.attIds == null) {
      sb.append("null");
    } else {
      sb.append(this.attIds);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("level:");
    if (this.level == null) {
      sb.append("null");
    } else {
      sb.append(this.level);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class EntsRelevanceSeekGraphGParamsStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public EntsRelevanceSeekGraphGParamsStandardScheme getScheme() {
      return new EntsRelevanceSeekGraphGParamsStandardScheme();
    }
  }

  private static class EntsRelevanceSeekGraphGParamsStandardScheme extends org.apache.thrift.scheme.StandardScheme<EntsRelevanceSeekGraphGParams> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, EntsRelevanceSeekGraphGParams struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // ENT_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.entName = iprot.readString();
              struct.setEntNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // ATT_IDS
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.attIds = iprot.readString();
              struct.setAttIdsIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // LEVEL
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.level = iprot.readString();
              struct.setLevelIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, EntsRelevanceSeekGraphGParams struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.entName != null) {
        oprot.writeFieldBegin(ENT_NAME_FIELD_DESC);
        oprot.writeString(struct.entName);
        oprot.writeFieldEnd();
      }
      if (struct.attIds != null) {
        oprot.writeFieldBegin(ATT_IDS_FIELD_DESC);
        oprot.writeString(struct.attIds);
        oprot.writeFieldEnd();
      }
      if (struct.level != null) {
        oprot.writeFieldBegin(LEVEL_FIELD_DESC);
        oprot.writeString(struct.level);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class EntsRelevanceSeekGraphGParamsTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public EntsRelevanceSeekGraphGParamsTupleScheme getScheme() {
      return new EntsRelevanceSeekGraphGParamsTupleScheme();
    }
  }

  private static class EntsRelevanceSeekGraphGParamsTupleScheme extends org.apache.thrift.scheme.TupleScheme<EntsRelevanceSeekGraphGParams> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, EntsRelevanceSeekGraphGParams struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetEntName()) {
        optionals.set(0);
      }
      if (struct.isSetAttIds()) {
        optionals.set(1);
      }
      if (struct.isSetLevel()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetEntName()) {
        oprot.writeString(struct.entName);
      }
      if (struct.isSetAttIds()) {
        oprot.writeString(struct.attIds);
      }
      if (struct.isSetLevel()) {
        oprot.writeString(struct.level);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, EntsRelevanceSeekGraphGParams struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.entName = iprot.readString();
        struct.setEntNameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.attIds = iprot.readString();
        struct.setAttIdsIsSet(true);
      }
      if (incoming.get(2)) {
        struct.level = iprot.readString();
        struct.setLevelIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}
